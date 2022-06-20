# Percona Query Analytics API

[![Travis CI Build Status](https://travis-ci.org/percona/qan-api.svg?branch=master)](https://travis-ci.org/percona/qan-api)
[![GoDoc](https://godoc.org/github.com/shatteredsilicon/qan-api?status.svg)](https://godoc.org/github.com/shatteredsilicon/qan-api)
[![Report Card](http://goreportcard.com/badge/percona/qan-api)](http://goreportcard.com/report/percona/qan-api)
[![CLA assistant](https://cla-assistant.percona.com/readme/badge/percona/qan-api)](https://cla-assistant.percona.com/percona/qan-api)

Percona Query Analytics (QAN) API is part of Percona Monitoring and Management (PMM).
See the [PMM docs](https://www.percona.com/doc/percona-monitoring-and-management/index.html) for more information.

##Building

In the empty dir run:
```
export GOPATH=$(pwd)
git clone http://github.com/shatteredsilicon/qan-api ./src/github.com/shatteredsilicon/qan-api
go build -o ./revel ./src/github.com/shatteredsilicon/qan-api/vendor/github.com/revel/cmd/revel
ln -s $(pwd)/src/github.com/shatteredsilicon/qan-api/vendor/github.com/revel src/github.com/revel
./revel build github.com/shatteredsilicon/qan-api <destination dir> prod
```
## Submitting Bug Reports

If you find a bug in Percona QAN API or one of the related projects, you should submit a report to that project's [JIRA](https://jira.percona.com) issue tracker.

Your first step should be [to search](https://jira.percona.com/issues/?jql=project+%3D+PMM+AND+component+%3D+%22QAN+App%22) the existing set of open tickets for a similar report. If you find that someone else has already reported your problem, then you can upvote that report to increase its visibility.

If there is no existing report, submit a report following these steps:

1. [Sign in to Percona JIRA.](https://jira.percona.com/login.jsp) You will need to create an account if you do not have one.
2. [Go to the Create Issue screen and select the relevant project.](https://jira.percona.com/secure/CreateIssueDetails!init.jspa?pid=11600&issuetype=1&priority=3&components=11711)
3. Fill in the fields of Summary, Description, Steps To Reproduce, and Affects Version to the best you can. If the bug corresponds to a crash, attach the stack trace from the logs.

An excellent resource is [Elika Etemad's article on filing good bug reports.](http://fantasai.inkedblade.net/style/talks/filing-good-bugs/).

As a general rule of thumb, please try to create bug reports that are:

- *Reproducible.* Include steps to reproduce the problem.
- *Specific.* Include as much detail as possible: which version, what environment, etc.
- *Unique.* Do not duplicate existing tickets.
- *Scoped to a Single Bug.* One bug per report.
