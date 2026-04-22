#
# atmsort.pm - Test output processor for pg_orca regression tests.
#
# Ported and simplified from Cloudberry/Greenplum's atmsort.pm:
#   - Removed MPP/segment-specific substitutions
#   - Removed explain.pm dependency (format_explain prefixes with GP_IGNORE: directly)
#   - Retained: init_file, ignore_plans, row sorting, start/end_ignore, match_subs/ignore
#
# Public interface:
#   atmsort_init(%args)      # IGNORE_PLANS, INIT_FILES, VERBOSE
#   run($filename)           # Returns temp filename with processed output
#   run_fhs($infh, $outfh)  # Process between file handles
#
package atmsort;

use strict;
use warnings;
use File::Temp qw/ tempfile /;

my $glob_ignore_plans;
my @glob_init;
my $glob_verbose;
my $glob_orderwarn;
my $glob_fqo;

my $atmsort_outfh;

my $glob_match_then_sub_fnlist;
my $glob_match_then_ignore_fnlist;

# -----------------------------------------------------------------------
# Public: initialize module state
# -----------------------------------------------------------------------
sub atmsort_init
{
    my %args = (
        IGNORE_PLANS => 0,
        INIT_FILES   => [],
        ORDER_WARN   => 0,
        VERBOSE      => 0,
        @_
    );

    $glob_ignore_plans = $args{IGNORE_PLANS};
    @glob_init         = @{$args{INIT_FILES}};
    $glob_orderwarn    = $args{ORDER_WARN};
    $glob_verbose      = $args{VERBOSE};
    $glob_fqo          = {count => 0};

    init_match_subs();
    init_matchignores();
    _process_init_files();
}

# -----------------------------------------------------------------------
# Load init files (each is processed through bigloop to register their
# match/substitution or match/ignore directives).
# -----------------------------------------------------------------------
sub _process_init_files
{
    return unless @glob_init;

    open my $devnullfh, '>', '/dev/null' or die "can't open /dev/null: $!";

    for my $init_file (@glob_init)
    {
        die "no such file: $init_file" unless -e $init_file;
        open my $fh, '<', $init_file or die "could not open $init_file: $!";
        atmsort_bigloop($fh, $devnullfh);
        close $fh;
    }

    close $devnullfh;
}

# -----------------------------------------------------------------------
# Build compiled match/substitution lambdas from a text block.
# Format (one pair per substitution):
#   m/match_regex/flags
#   s/match_regex/replacement/flags
# Blank lines and # comments are ignored.
# -----------------------------------------------------------------------
sub _build_match_subs
{
    my ($text, $label) = @_;

    # Strip comments and blank lines
    $text =~ s/^\s*(?:#.*)?(?:[\r\n]|\x0D\x0A)//gm;

    my @lines = split(/\n/, $text);
    my @pairs;
    my $pending;

    for my $line (@lines)
    {
        if (defined $pending)
        {
            push @pairs, [$pending, $line];
            undef $pending;
        }
        else
        {
            $pending = $line;
        }
    }

    my $mscount = 1;
    for my $pair (@pairs)
    {
        unless (2 == scalar @{$pair})
        {
            warn "bad matchsubs definition in $label";
            next;
        }

        my $code = sprintf(
            'sub { my $ini = shift; if ($ini =~ %s) { $ini =~ %s; } return $ini; }',
            $pair->[0], $pair->[1]
        );
        my $fn = eval $code;
        if ($@)
        {
            warn "bad eval in $label matchsubs: $@\n$code";
            next;
        }
        my $cmt = "$label matchsubs #$mscount";
        $mscount++;
        push @{$glob_match_then_sub_fnlist}, [$fn, $code, $cmt, $pair->[0], $pair->[1]];
    }
    return [1];
}

# Default substitutions (none needed for single-node PG)
sub init_match_subs
{
    $glob_match_then_sub_fnlist = [];
    # No default substitutions for pg_orca (no MPP-specific output)
}

sub match_then_subs
{
    my $ini = shift;
    for my $ff (@{$glob_match_then_sub_fnlist})
    {
        $ini = $ff->[0]->($ini);
    }
    return $ini;
}

# -----------------------------------------------------------------------
# Build compiled match/ignore lambdas.
# Format: one m/regex/flags per line. Lines matching are dropped.
# -----------------------------------------------------------------------
sub _build_match_ignores
{
    my ($text, $label) = @_;

    $text =~ s/^\s*(?:#.*)?(?:[\r\n]|\x0D\x0A)//gm;
    my @exprs = split(/\n/, $text);

    my $mscount = 1;
    for my $expr (@exprs)
    {
        next unless length $expr;
        my $code = "sub { my \$ini = shift; return (\$ini =~ $expr); }";
        my $fn = eval $code;
        if ($@)
        {
            warn "bad eval in $label matchignore: $@\n$code";
            next;
        }
        my $cmt = "$label matchignores #$mscount";
        $mscount++;
        push @{$glob_match_then_ignore_fnlist}, [$fn, $code, $cmt, $expr, '(ignore)'];
    }
    return [1];
}

sub init_matchignores
{
    $glob_match_then_ignore_fnlist = [];
    # No default ignore patterns for pg_orca
}

# Returns 1 if line should be dropped, 0 to keep
sub match_then_ignore
{
    my $ini = shift;
    for my $ff (@{$glob_match_then_ignore_fnlist})
    {
        return 1 if $ff->[0]->($ini);
    }
    return 0;
}

# -----------------------------------------------------------------------
# tablelizer: parse a psql table into an array of column hashes.
# Columns are 1-indexed.
# -----------------------------------------------------------------------
sub tablelizer
{
    my ($text, $firstline_override) = @_;

    my @lines = split(/\n/, $text);
    return undef unless scalar @lines;

    my $hdrline = defined($firstline_override) ? $firstline_override : shift @lines;
    my @colheads = split(/\s+\|\s+/, $hdrline);
    $colheads[0]  =~ s/^\s+|\s+$//g;
    $colheads[-1] =~ s/^\s+|\s+$//g;

    return undef unless scalar @lines;
    shift @lines unless defined $firstline_override;  # skip dashed separator

    my @rows;
    for my $line (@lines)
    {
        my @cols = split(/\|/, $line, scalar @colheads);
        last unless scalar(@cols) == scalar(@colheads);

        my $rowh = {};
        for my $i (0 .. $#colheads)
        {
            my $val = shift @cols;
            $val =~ s/^\s+|\s+$//g;
            $rowh->{$i + 1} = $val;
        }
        push @rows, $rowh;
    }
    return \@rows;
}

# -----------------------------------------------------------------------
# format_explain: handle EXPLAIN plan output.
#
# When --ignore-plans: prefix every line with GP_IGNORE: so diff ignores them.
# Otherwise: pass through as-is (plan lines are already in @outarr).
# -----------------------------------------------------------------------
sub format_explain
{
    my ($outarr, $directive) = @_;
    $directive = {} unless defined $directive;

    my $ignore = $glob_ignore_plans || exists($directive->{ignore});

    for my $line (@{$outarr})
    {
        if ($ignore)
        {
            print $atmsort_outfh "GP_IGNORE:", $line;
        }
        else
        {
            print $atmsort_outfh $line;
        }
    }
}

# -----------------------------------------------------------------------
# format_query_output: sort (or not) a result set and emit it.
# -----------------------------------------------------------------------
sub format_query_output
{
    my ($fqostate, $has_order, $outarr, $directive) = @_;
    $directive = {} unless defined $directive;

    $fqostate->{count} += 1;

    # EXPLAIN output
    if (exists $directive->{explain})
    {
        # Always route through format_explain so --ignore-plans works uniformly.
        format_explain($outarr, $directive);
        return;
    }

    my $prefix = '';
    $prefix = 'GP_IGNORE:' if exists $directive->{ignore};

    # Partial-order / mvd directives: use tablelizer sort
    if (exists $directive->{sortlines})
    {
        my $firstline  = $directive->{firstline};
        my $ordercols  = $directive->{order};
        my $mvdlist    = $directive->{mvd};
        my $lines_text = join('', @{$outarr});
        my $ah1        = tablelizer($lines_text, $firstline);

        unless (defined($ah1) && scalar @{$ah1})
        {
            return;
        }

        my @allcols    = sort keys %{$ah1->[0]};
        my @presortcols;

        if (defined($ordercols) && length($ordercols))
        {
            $ordercols =~ s/\n|\s//gm;
            @presortcols = split(/\s*,\s*/, $ordercols);
        }

        # Clear if "order 0"
        if (1 == scalar(@presortcols) && $presortcols[0] eq '0')
        {
            @presortcols = ();
        }

        my %unsorth = map { $_ => 1 } @allcols;
        delete $unsorth{$_} for @presortcols;
        my @unsortcols = sort keys %unsorth;

        if (@presortcols)
        {
            for my $h_row (@{$ah1})
            {
                my @vals = map { $h_row->{$_} // '' } @presortcols;
                print $atmsort_outfh $prefix, join(' | ', @vals), "\n";
            }
        }

        my @finalunsort;
        if (@unsortcols)
        {
            for my $h_row (@{$ah1})
            {
                push @finalunsort, join(' | ', map { $h_row->{$_} // '' } @unsortcols);
            }
            for my $line (sort @finalunsort)
            {
                print $atmsort_outfh $prefix, $line, "\n";
            }
        }
        return;
    }

    if ($has_order)
    {
        # Preserve order as-is
        for my $line (@{$outarr})
        {
            print $atmsort_outfh $prefix, $line;
        }
    }
    else
    {
        # Sort for unordered output
        for my $line (sort @{$outarr})
        {
            print $atmsort_outfh $prefix, $line;
        }
    }
}

# -----------------------------------------------------------------------
# atmsort_bigloop: main processing loop
# -----------------------------------------------------------------------
sub atmsort_bigloop
{
    my ($infh, $outfh_arg) = @_;
    $atmsort_outfh = $outfh_arg;

    my $sql_statement  = '';
    my @outarr;
    my $getrows        = 0;
    my $getstatement   = 0;
    my $has_order      = 0;
    my $describe_mode  = 0;
    my $directive      = {};
    my $big_ignore     = 0;
    my %define_match_expression;

    print $atmsort_outfh "GP_IGNORE: formatted by atmsort.pm\n";

  L_bigwhile:
    while (my $ini = <$infh>)
    {
      reprocess_row:

        # Inside a start_matchsubs / start_matchignore block
        if (%define_match_expression)
        {
            if ($ini =~ m/--\s*end_match(subs|ignore)\s*$/)
            {
                my $type = $1;
                my $raw  = $define_match_expression{expr};

                # Split off the opening "-- start_match..." line, keep the rest.
                my (undef, $doc) = split(/\n/, $raw, 2);
                $doc //= '';

                # Strip leading SQL comment markers (-- ) from each line.
                $doc =~ s/^\s*--\s?//gm;

                if ($type eq 'subs')
                {
                    _build_match_subs($doc, 'USER');
                }
                else
                {
                    _build_match_ignores($doc, 'USER');
                }
                undef %define_match_expression;
                print $atmsort_outfh "GP_IGNORE: defined new match expression\n";
            }
            else
            {
                $define_match_expression{expr} .= $ini;
                push @outarr, $ini;
            }
            next;
        }

        # Inside start_ignore / end_ignore block
        if ($big_ignore > 0)
        {
            $big_ignore-- if $ini =~ m/--\s*end_ignore\s*$/;
            print $atmsort_outfh "GP_IGNORE:", $ini;
            next;
        }

        # Apply match/sub rules
        $ini = match_then_subs($ini);

        # Drop matching ignore lines
        next if !$ini || match_then_ignore($ini);

        if ($getrows)
        {
            my $end_of_table = 0;

            if ($describe_mode)
            {
                if ($ini =~ m/^$/)
                {
                    $end_of_table = 1;
                }
                elsif (exists $directive->{firstline})
                {
                    my $hdr_sep = ($directive->{firstline} =~ tr/|//);
                    my $cur_sep = ($ini =~ tr/|//);
                    $end_of_table = 1 if $hdr_sep != $cur_sep;
                }
            }

            if ($ini =~ m/^\s*\(\d+\s+rows?\)\s*$/)
            {
                # Ignore row count line for EXPLAIN (it varies)
                if (exists $directive->{explain})
                {
                    $ini = 'GP_IGNORE:' . $ini;
                }
                $end_of_table = 1;
            }

            if ($end_of_table)
            {
                format_query_output($glob_fqo, $has_order, \@outarr, $directive);
                $directive     = {};
                @outarr        = ();
                $getrows       = 0;
                $has_order     = 0;
                $describe_mode = 0;
            }
        }
        else
        {
            # Detect start_matchsubs / start_matchignore
            if ($ini =~ m/--\s*start_match(subs|ignore)\s*$/)
            {
                $define_match_expression{type} = $1;
                $define_match_expression{expr} = $ini;
                push @outarr, $ini;
                next;
            }

            # start_ignore
            if ($ini =~ m/--\s*start_ignore\s*$/)
            {
                $big_ignore++;
                print $atmsort_outfh $_, 'GP_IGNORE:', $ini for @outarr;
                @outarr = ();
                print $atmsort_outfh 'GP_IGNORE:', $ini;
                next;
            }

            # EXPLAIN (COSTS OFF) detection
            if ($ini =~ m/explain\s*\(.*costs\s+(?:off|false|0).*\)/i)
            {
                $directive->{explain} = 'costs_off';
            }
            elsif ($ini =~ m/(?:insert|update|delete|select|^\s*\\d|copy|execute)/i)
            {
                $has_order = 0;
                $sql_statement = '';

                if ($ini =~ m/explain.*(?:insert|update|delete|select|execute)/i)
                {
                    $directive->{explain} = 'normal';
                }

                $describe_mode = ($ini =~ m/^\s*\\d/);
            }

            # In-line directives: -- order N, -- ignore, -- mvd, -- force_explain,
            #                     -- explain_processing_on/off, -- order none
            if ($ini =~ m/--\s*((force_explain)\s*(operator)?\s*$|(ignore)\s*$|(order)\s+(\d+|none).*$|(mvd)\s+\d+.*$|(explain_processing_(on|off))\s*$)/)
            {
                my $first = substr($1, 0, 1);
                if ($first eq 'i')
                {
                    $directive->{ignore} = 'ignore';
                }
                elsif ($first eq 'o')
                {
                    my $olist = $ini;
                    $olist =~ s/^.*--\s*order//;
                    if ($olist =~ /none/)
                    {
                        $directive->{order_none} = 1;
                    }
                    else
                    {
                        $directive->{order} = $olist;
                    }
                }
                elsif ($first eq 'f')
                {
                    $directive->{explain} = defined($3) ? 'operator' : 'normal';
                }
                elsif ($first eq 'e')
                {
                    $1 =~ m/(on|off)$/;
                    $directive->{explain_processing} = $1;
                }
                else
                {
                    my $olist = $ini;
                    $olist =~ s/^.*--\s*mvd//;
                    $directive->{mvd} = $olist;
                }
            }

            # Track SQL for ORDER BY detection
            if ($ini =~ m/select/i)
            {
                $getstatement = 1;
                $sql_statement .= $ini;
            }
            $getstatement = 0 if index($ini, ';') != -1;

            # Detect start of psql result table:
            # previous outarr[-1] is header " col1 | col2 "
            # current line is "---+---"
            if (scalar(@outarr) > 1
                && $outarr[-1] =~ m/^\s+.*\s$/
                && $ini =~ m/^(?:--+-(?:\+-+)*)$/)
            {
                $directive->{firstline} = $outarr[-1];

                if (exists($directive->{order}) || exists($directive->{mvd}))
                {
                    $directive->{sortlines} = $outarr[-1];
                }

                # Normalize EXPLAIN header line
                if (exists($directive->{explain})
                    && $ini =~ m/^\s*(?:--+-)+\s*$/
                    && scalar(@outarr) && $outarr[-1] =~ m/QUERY PLAN/)
                {
                    $outarr[-1] = "QUERY PLAN\n";
                    $ini = ('_' x length($outarr[-1])) . "\n";
                }

                $getstatement = 0;

                print $atmsort_outfh $_ for @outarr;
                @outarr = ();
                print $atmsort_outfh $ini;

                # Determine if result is ordered
                if (defined $directive->{explain})
                {
                    $has_order = 1;
                }
                elsif (defined($sql_statement) && length($sql_statement)
                    && !defined($directive->{order_none})
                    && $sql_statement =~ m/select.*order.*by/is)
                {
                    # Exclude window/within-group ORDER BY
                    my $t = $sql_statement;
                    $t =~ s/over\s*\((?:partition\s+by.*?)?order\s+by/xx/isg;
                    $t =~ s/window\s+\w+\s+as\s*\((?:partition\s+by.*?)?order\s+by/xx/isg;
                    $t =~ s/within\s+group\s*\(order\s+by.*?\)/xx/isg;
                    # Strip all parenthesized groups (subqueries) so that
                    # ORDER BY inside a subquery does not fool the detector.
                    1 while $t =~ s/\([^()]*\)//g;
                    $has_order = ($t =~ m/order\s+by/is) ? 1 : 0;
                }
                else
                {
                    $has_order = 0;
                }

                $directive->{sql_statement} = $sql_statement;
                $sql_statement = '';
                $getrows = 1;
                next;
            }
        }

        push @outarr, $ini;
    }

    # Flush remaining lines
    print $atmsort_outfh $_ for @outarr;
}

# -----------------------------------------------------------------------
# Public: run on a file, return temp output filename
# -----------------------------------------------------------------------
sub run
{
    my $infname = shift;
    open my $infh, '<', $infname or die "could not open $infname: $!";
    my ($outfh, $outfname) = tempfile();
    run_fhs($infh, $outfh);
    close $infh;
    close $outfh;
    return $outfname;
}

# Public: run between file handles
sub run_fhs
{
    my ($infh, $outfh) = @_;
    atmsort_bigloop($infh, $outfh);
}

1;
