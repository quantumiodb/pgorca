# Contributors

pg_orca embeds the **ORCA query optimizer**, whose four core libraries
(`libgpos`, `libnaucrates`, `libgpopt`, `libgpdbcost`) are taken from the
`src/backend/gporca` tree of [Apache Cloudberry](https://github.com/apache/cloudberry).
The optimizer is the work of many people, and pg_orca would not exist
without them. This file records that debt.

## Original ORCA authors (Greenplum / Pivotal / VMware)

ORCA was designed and built by the Greenplum query-optimizer team starting
in 2004 (see [`src/backend/gporca/COPYRIGHT`](https://github.com/apache/cloudberry/blob/main/src/backend/gporca/COPYRIGHT),
"Copyright (c) 2004–2015 VMware, Inc. or its affiliates"). Its architecture
is described in the SIGMOD 2014 paper *"Orca: A Modular Query Optimizer
Architecture for Big Data"* by M. Soliman, L. Antova, V. Raghavan,
A. El-Helw, Z. Gu, E. Shen, G. Caragea, C. Garcia-Alvarado, F. Rahman,
M. Petropoulos, F. Waas, S. Narayanan, K. Krikellas, and R. Baldwin.
The per-commit history of that early era predates the Cloudberry
repository and is not reproducible from git; we acknowledge those authors
here collectively.

## Cloudberry ORCA contributors

The following people authored commits touching `src/backend/gporca` in the
Apache Cloudberry repository. Listed alphabetically; GitHub profiles link
where the commit author is associated with a GitHub account.

<!-- BEGIN GENERATED LIST -->
- [Abhijit Subramanya](https://github.com/asubramanya)
- [Alena Rybakina](https://github.com/Alena0704)
- [Alexandr Barulev](https://github.com/HustonMmmavr)
- [Alexandra Wang](https://github.com/l-wang)
- Alexey Gordeev
- [Amit Khandekar](https://github.com/amitdkhan-pg)
- [Anusha Shakarad](https://github.com/Shakarada)
- [Ashuka Xue](https://github.com/ashuka24)
- [bhari](https://github.com/hpbee)
- Bhuvnesh Chaudhary
- [Brent Doil](https://github.com/bmdoil)
- [Chandan Kunal](https://github.com/chandankunal)
- [chaotian](https://github.com/charliettxx)
- [Chen Mulong](https://github.com/beeender)
- [Chris Hajas](https://github.com/chrishajas)
- [Daniel Gustafsson](https://github.com/danielgustafsson)
- [Daniel Hoffman](https://github.com/thedanhoffman)
- [David Kimura](https://github.com/dgkimura)
- Denis Smirnov
- [Dev Chattopadhyay](https://github.com/DevChattopadhyay)
- [Dianjin Wang](https://github.com/tuhaihe)
- [Divyesh Vanjare](https://github.com/divyeshddv)
- Ekta Khanna
- [fishtree1161](https://github.com/fishtree1161)
- [Francis](https://github.com/Light-City)
- [Georgy Shelkovy](https://github.com/RekGRpth)
- [Haisheng Yuan](https://github.com/hsyuan)
- Hans Zeller
- [Hao Wu](https://github.com/gfphoenix78)
- [Hari krishna](https://github.com/pobbatihari)
- [Heikki Linnakangas](https://github.com/hlinnaka)
- [hilm](https://github.com/chenjinbao1989)
- [Hubert Zhang](https://github.com/zhangh43)
- Jesse Zhang
- [Jianghua Yang](https://github.com/yjhjstz)
- Jingyu Wang
- [Leonid Borchuk](https://github.com/leborchuk)
- Maksim Milyutin
- [Maxim Smyatkin](https://github.com/Smyatkin-Maxim)
- [Melanie Plageman](https://github.com/melanieplageman)
- [NISHANT SHARMA](https://github.com/24nishant)
- [oracle](https://github.com/oracleloyall)
- [Orhan Kislal](https://github.com/orhankislal)
- [Pan Wang](https://github.com/wangpanCN)
- [PJ Fanning](https://github.com/pjfanning)
- [Robert Mu](https://github.com/robertmu)
- [Sambitesh Dash](https://github.com/sambitesh)
- [Sanath Kumar Vobilisetty](https://github.com/Sanath97)
- [Shreedhar Hardikar](https://github.com/hardikar)
- [Soumyadeep Chakraborty](https://github.com/soumyadeep2007)
- [Tao Tang](https://github.com/Tao-T)
- [terry](https://github.com/terry-chelsea)
- [THANATOSLAVA](https://github.com/THANATOSLAVA)
- [Tyler Ramer](https://github.com/tylarb)
- [wangxiaoran](https://github.com/fanfuxiaoran)
- [Wu Ning](https://github.com/50wu)
- [wuyuhao28](https://github.com/wuyuhao28)
- [Yongtao Huang](https://github.com/hyongtao-code)
- [Zhang Mingli](https://github.com/avamingli)
- [zhoujiaqi](https://github.com/jiaqizho)
<!-- END GENERATED LIST -->

## How this list was generated

The Cloudberry list is derived from the public commit history of the
`src/backend/gporca` subtree, deduplicated by author name and GitHub login
(fetched 2026-07-04). It reflects the history available in the Apache
Cloudberry repository and does not include pre-Cloudberry Greenplum history,
which is no longer published as git. To regenerate:

```bash
for p in $(seq 1 6); do
  curl -s "https://api.github.com/repos/apache/cloudberry/commits?path=src/backend/gporca&per_page=100&page=$p"
done
```

If your name is missing, misspelled, or you would like a different
attribution (or none at all), please open a pull request or an issue.
