#!/usr/bin/env perl
#
# gpdiff.pl - diff wrapper for pg_orca regression tests.
#
# Ported from Cloudberry/Greenplum's gpdiff.pl.
# Processes both files through atmsort.pm (sorting unordered query output,
# applying init-file substitutions, optionally ignoring EXPLAIN plans),
# then runs standard diff on the processed versions.
#
# Usage:
#   gpdiff.pl [diff-options] [--gpd_ignore_plans] [--gpd_init FILE ...] file1 file2
#
# Options:
#   --gpd_ignore_plans    Ignore EXPLAIN plan content (prefix with GP_IGNORE:)
#   --gpd_init FILE       Load substitution init file (repeatable)
#   --verbose             Print verbose atmsort info
#   All other options are passed through to diff.
#
# Exit status mirrors diff: 0=identical, 1=different, 2=error.

use strict;
use warnings;
use File::Spec;
use Getopt::Long qw(GetOptions);
Getopt::Long::Configure qw(pass_through);

use FindBin;
use lib "$FindBin::Bin";
use atmsort;

my %atmsort_args;

GetOptions(
    'gpd_ignore_plans|gp_ignore_plans' => \$atmsort_args{IGNORE_PLANS},
    'gpd_init|gp_init_file=s'          => \@{$atmsort_args{INIT_FILES}},
    'verbose|Verbose'                   => \$atmsort_args{VERBOSE},
    'version|v'                         => sub { print_version(); exit 0 },
    'help|h'                            => sub { usage(); exit 1 },
);

if (@ARGV < 2)
{
    print STDERR "gpdiff.pl: need at least two files to compare\n";
    usage();
    exit 2;
}

my $f2 = pop @ARGV;
my $f1 = pop @ARGV;

for my $f ($f1, $f2)
{
    unless (-e $f)
    {
        print STDERR "gpdiff.pl: $f: No such file or directory\n";
    }
}
exit 2 unless -e $f1 && -e $f2;

exit filefunc($f1, $f2);

# ---------------------------------------------------------------------------

sub gpdiff_files
{
    my ($f1, $f2, $d2d) = @_;

    atmsort::atmsort_init(%atmsort_args);
    my $tmp1 = atmsort::run($f1);
    my $tmp2 = atmsort::run($f2);

    # Remaining @ARGV holds pass-through diff options (e.g. -w -I "NOTICE:")
    # Always ignore GP_IGNORE: lines so that prefixed plan/ignored output is
    # transparent to diff regardless of which side added the prefix.
    my $extra_opts = join(' ', @ARGV, '-I', 'GP_IGNORE:');
    my $out = `/usr/bin/diff $extra_opts "$tmp1" "$tmp2"`;
    my $stat = $? >> 8;

    if (defined($d2d) && length($out))
    {
        $out = "diff $f1 $f2\n" . $out;
    }

    # Replace temp file paths with the real names in the diff output
    $out =~ s/\Q$tmp1\E/$f1/gm;
    $out =~ s/\Q$tmp2\E/$f2/gm;

    print $out;

    unlink $tmp1;
    unlink $tmp2;

    return $stat;
}

sub filefunc
{
    my ($f1, $f2, $d2d) = @_;

    if (-f $f1 && -f $f2)
    {
        return gpdiff_files($f1, $f2, $d2d);
    }

    if (-d $f1 && -d $f2)
    {
        my $stat = 0;
        opendir my $dh, $f1 or return 0;
        while (my $name = readdir $dh)
        {
            next if $name eq '.' || $name eq '..';
            my $abs = File::Spec->rel2abs(File::Spec->catfile($f1, $name));
            $d2d = {} unless defined $d2d;
            $d2d->{dir} = 1;
            $stat = filefunc($abs, $f2, $d2d);
        }
        closedir $dh;
        return $stat;
    }

    if (-f $f1 && -d $f2)
    {
        my @parts = File::Spec->splitpath($f1);
        return 0 unless @parts;
        my $base = $parts[-1];
        return filefunc($f1, File::Spec->rel2abs(File::Spec->catfile($f2, $base)), $d2d);
    }

    if (-f $f2 && -d $f1)
    {
        my @parts = File::Spec->splitpath($f2);
        return 0 unless @parts;
        my $base = $parts[-1];
        return filefunc(File::Spec->rel2abs(File::Spec->catfile($f1, $base)), $f2, $d2d);
    }

    return 0;
}

sub print_version
{
    print "gpdiff.pl 1.0 (pg_orca)\n";
    my $dv = `/usr/bin/diff --version 2>&1 | head -1`;
    print $dv if defined $dv;
}

sub usage
{
    print <<'USAGE';
Usage: gpdiff.pl [options] file1 file2

Options:
  --gpd_ignore_plans    Ignore EXPLAIN plan output differences
  --gpd_init FILE       Load init file with match/substitution rules
  --verbose             Verbose output
  --version             Print version
  --help                This help

All other options are passed through to diff.

Init file format (pairs of perl regex operators, one per line):
  m/pattern/
  s/pattern/replacement/

Example:
  gpdiff.pl --gpd_ignore_plans -w expected.out results.out
  gpdiff.pl --gpd_init cost.init -w expected.out results.out
USAGE
}
