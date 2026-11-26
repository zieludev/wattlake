# Third-party components

Components this repository depends on or refers to. None of their code is copied into the
repository.

| source | name | license type | license link |
| --- | --- | --- | --- |
| https://github.com/apache/spark | pyspark (Apache Spark for Python, with Spark's jars) | Apache-2.0 | https://github.com/apache/spark/blob/master/LICENSE |
| https://github.com/py4j/py4j | py4j (the bridge between Python and Spark's JVM) | BSD-3-Clause | https://github.com/py4j/py4j/blob/master/LICENSE.txt |
| https://github.com/pandas-dev/pandas | pandas | BSD-3-Clause | https://github.com/pandas-dev/pandas/blob/main/LICENSE |
| https://github.com/numpy/numpy | numpy | BSD-3-Clause | https://github.com/numpy/numpy/blob/main/LICENSE.txt |
| https://github.com/apache/arrow | pyarrow | Apache-2.0 | https://github.com/apache/arrow/blob/main/LICENSE.txt |
| https://github.com/pytest-dev/pytest | pytest (tests) | MIT | https://github.com/pytest-dev/pytest/blob/main/LICENSE |
| https://github.com/astral-sh/ruff | ruff (lint and format) | MIT | https://github.com/astral-sh/ruff/blob/main/LICENSE |
| https://github.com/DetachHead/basedpyright | basedpyright (type check) | MIT | https://github.com/DetachHead/basedpyright/blob/main/LICENSE.txt |
| https://github.com/astral-sh/uv | uv_build (build backend), and uv in the Docker image | MIT OR Apache-2.0 | https://github.com/astral-sh/uv/blob/main/LICENSE-MIT |
| https://github.com/actions/checkout | actions/checkout (CI only) | MIT | https://github.com/actions/checkout/blob/main/LICENSE |
| https://github.com/astral-sh/setup-uv | astral-sh/setup-uv (CI only) | MIT | https://github.com/astral-sh/setup-uv/blob/main/LICENSE |
| https://osf.io/6hfpz/ | GoldenCheetah OpenData (the data; one athlete of it is in `tests/data`) | CC0-1.0 | https://creativecommons.org/publicdomain/zero/1.0/legalcode |

Their own dependencies are pinned in `uv.lock`. Going by the metadata each package publishes,
every one of them is under a permissive licence: MIT, Apache-2.0, a BSD licence, 0BSD, Zlib or
CC0-1.0.

## Spark's jars

The pyspark package carries the jars of Apache Spark, and with them the jars Spark itself is
built on. Spark's own LICENSE and NOTICE, installed with the package, list each of them. Most
are under Apache-2.0, MIT or a BSD licence. A few are under weak copyleft licences that the
Apache Software Foundation allows in its binary releases: CDDL-1.0 and CDDL-1.1 (the
`javax.transaction` APIs, and `javax.servlet-api`, also offered under GPL-2.0 with the
Classpath Exception) and EPL-2.0 (the Jakarta annotation, servlet and REST APIs, and GlassFish
HK2). This project runs them as Spark ships them, through Spark's public API, and does not
change or redistribute them: they arrive when pyspark is installed.

## The Docker image

The image starts from the official `python:3.13-slim-trixie` image, which is Debian, and adds
two Debian packages: `openjdk-21-jre-headless`, the Java runtime Spark needs (GPL-2.0 with the
Classpath Exception), and `libnss-wrapper` (BSD-3-Clause). It builds them into an image and
runs them unmodified; none of them is part of the repository. The CI workflows use the Java 21
that GitHub preinstalls on its Ubuntu runners.

## Things this project does not use

The GoldenCheetah application, which computed the metrics stored in the data, is under the GPL.
No code of it is used or copied here: the formulas in `src/wattlake` are written from their
published definitions, and GoldenCheetah's stored results serve only as numbers to compare
against.
