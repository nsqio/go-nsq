## go-nsq

`go-nsq` is the official Go package for [NSQ][nsq].

The latest stable release is [0.3.4](https://github.com/bitly/go-nsq/releases/tag/v0.3.4).

[![Build Status](https://secure.travis-ci.org/bitly/go-nsq.png?branch=master)](http://travis-ci.org/bitly/go-nsq)

It provides high-level [Reader][reader] and [Writer][writer] types to implement consumers and
producers as well as low-level functions to communicate over the [NSQ protocol][protocol].

See the [main repo apps][apps] directory for examples of clients built using this package.

### Installing

    $ go get github.com/bitly/go-nsq

### Docs

See [godoc][nsq_gopkgdoc] for pretty documentation or:

    # in the go-nsq package directory
    $ go doc

[nsq]: https://github.com/bitly/nsq
[nsq_gopkgdoc]: http://godoc.org/github.com/bitly/go-nsq
[protocol]: http://bitly.github.io/nsq/clients/tcp_protocol_spec.html
[apps]: https://github.com/bitly/nsq/tree/master/apps
[reader]: http://godoc.org/github.com/bitly/go-nsq#Reader
[writer]: http://godoc.org/github.com/bitly/go-nsq#Writer
