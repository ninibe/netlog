# Contributing

Contributions are more than welcome to help netlog move forward.

Before submitting major changes, here are a few guidelines to follow:

1. Check the [open issues][issues] and [pull requests][prs] for existing discussions.
1. Open an [issue][issues] to discuss a new feature.
1. Run the following linting tools:
	1. [golint](https://github.com/golang/lint/golint)
	1. [errcheck](https://github.com/kisielk/errcheck)
	1. [structcheck](https://github.com/opennota/check/cmd/structcheck)
	1. [varcheck](https://github.com/opennota/check/cmd/varcheck)
	1. [gosimple](https://honnef.co/go/simple/cmd/gosimple)
	1. [staticcheck](https://honnef.co/go/staticcheck/cmd/staticcheck)
	1. [goconst](https://github.com/jgautheron/goconst/cmd/goconst)
1. Write tests and make sure the entire test suite passes locally and on Travis CI.
1. When possible, prefix your commits with [`netlog:`, `biglog:`, `transport:`, `docs:`, `ci:`]
1. Open a Pull Request.

[issues]: https://github.com/ninibe/netlog/issues
[prs]: https://github.com/ninibe/netlog/pulls

For any further questions you can write to the [netlog-dev mailing list](https://groups.google.com/forum/#!forum/netlog-dev).
