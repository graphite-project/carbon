# Carbon

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/85221cd3bb6e49d7bbd6fed376a88264)](https://www.codacy.com/app/graphite-project/carbon?utm_source=github.com&utm_medium=referral&utm_content=graphite-project/carbon&utm_campaign=badger)
[![Build Status](https://secure.travis-ci.org/graphite-project/carbon.png?branch=master)](http://travis-ci.org/graphite-project/carbon)
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fgraphite-project%2Fcarbon.svg?type=shield)](https://app.fossa.io/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fgraphite-project%2Fcarbon?ref=badge_shield)
[![codecov](https://codecov.io/gh/graphite-project/carbon/branch/master/graph/badge.svg)](https://codecov.io/gh/graphite-project/carbon)

## Overview

Carbon is one of three components within the Graphite project:

1. [Graphite-Web](https://github.com/graphite-project/graphite-web), a Django-based web application that renders graphs and dashboards
2. The Carbon metric processing daemons
3. The [Whisper](https://github.com/graphite-project/whisper) time-series database library

![Graphite Components](https://github.com/graphite-project/graphite-web/raw/master/webapp/content/img/overview.png "Graphite Components")

Carbon is responsible for receiving metrics over the network, caching them in memory for "hot queries" from the Graphite-Web application, and persisting them to disk using the Whisper time-series library.

## Installation, Configuration and Usage

Please refer to the instructions at [readthedocs](http://graphite.readthedocs.org/).

## License

Carbon is licensed under version 2.0 of the Apache License. See the [LICENSE](https://github.com/graphite-project/carbon/blob/master/LICENSE) file for details.
