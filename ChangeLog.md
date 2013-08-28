## go-nsq Change Log

### 0.3.2 - 2013-08-26

**Upgrading from 0.3.1**: This release requires NSQ binary version `0.2.22+` for TLS support.

New Features/Improvements:

 * #227 - TLS feature negotiation
 * #164/#202/#255 - add `Writer`
 * #186 - `MaxBackoffDuration` of `0` disables backoff
 * #175 - support for `nsqd` config option `--max-rdy-count`
 * #169 - auto-reconnect to hard-coded `nsqd`

Bug Fixes:

 * #254/#256/#257 - new connection RDY starvation
 * #250 - `nsqlookupd` polling improvements
 * #243 - limit `IsStarved()` to connections w/ in-flight messages
 * #169 - use last RDY count for `IsStarved()`; redistribute RDY state
 * #204 - fix early termination blocking
 * #177 - support `broadcast_address`
 * #161 - connection pool goroutine safety

### 0.3.1 - 2013-02-07

**Upgrading from 0.3.0**: This release requires NSQ binary version `0.2.17+` for `TOUCH` support.

 * #119 - add TOUCH command
 * #133 - improved handling of errors/magic
 * #127 - send IDENTIFY (missed in #90)
 * #16 - add backoff to Reader

### 0.3.0 - 2013-01-07

**Upgrading from 0.2.4**: There are no backward incompatible changes to applications
written against the public `nsq.Reader` API.

However, there *are* a few backward incompatible changes to the API for applications that 
directly use other public methods, or properties of a few NSQ data types:

`nsq.Message` IDs are now a type `nsq.MessageID` (a `[16]byte` array).  The signatures of
`nsq.Finish()` and `nsq.Requeue()` reflect this change.

`nsq.SendCommand()` and `nsq.Frame()` were removed in favor of `nsq.SendFramedResponse()`.

`nsq.Subscribe()` no longer accepts `shortId` and `longId`.  If upgrading your consumers
before upgrading your `nsqd` binaries to `0.2.16-rc.1` they will not be able to send the 
optional custom identifiers.
    
 * #90 performance optimizations
 * #81 reader performance improvements / MPUB support

### 0.2.4 - 2012-10-15

 * #69 added IsStarved() to reader API

### 0.2.3 - 2012-10-11

 * #64 timeouts on reader queries to lookupd
 * #54 fix crash issue with reader cleaning up from unexpectedly closed nsqd connections

### 0.2.2 - 2012-10-09

 * Initial public release
