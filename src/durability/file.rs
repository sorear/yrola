// Layout
// /yrola_database # semver; lock target
// /journals/NNN # named for first transid, consists of a sequence of lowcommit, changefile records
// /snapshots/NNN # contains a cumulative representation of all txns less than NNN
// /objects/stable/YYY
// /objects/pending/YYY.NNN.add # will be added in txn NNN
// /objects/pending/YYY.prewrite # not yet in a txn
// /objects/pending/YYY.NNN.del # will be deleted in txn NNN

// On startup, check and lock yrola_database, then read the journal files in order for
// syntactically valid commits.  The last commit may be syntactically valid but incomplete because
// the files it adds were not successfully flushed; check those too (check pending adds, stable,
// and pending delete because the next commit could have started to remove the file.  It is not
// allowed to add and remove the same file in the same commit.)  Truncate and reextend the journal
// to avoid nonzero data from the last run creating problems.

// If snapshot NNN exists and is complete, then journals and snapshots <NNN are redundant and
// should be deleted.  Incomplete snapshots should be deleted on startup.
