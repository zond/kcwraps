kcwraps
=======

Some wrappers around <a href="http://fallabs.com/kyotocabinet/">Kyoto Cabinet</a>.

## Bits

* http://godoc.org/github.com/zond/kcwraps/kc
 * Provides sorted set functionality on top of https://bitbucket.org/ww/cabinet, among them optimized set operations (intersection, union, difference etc) from https://github.com/zond/setop.
* http://godoc.org/github.com/zond/kcwraps/kol
 * Provides a objekt layer on top of http://godoc.org/github.com/zond/kcwraps/kc, by making it simple to serialize/unserialize structs into the cabinet. Also provides automatic indexing functionality for query goodness, and a subscription API for event based updating of clients.
