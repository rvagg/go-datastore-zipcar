# go-ds-zipcar

An implementation of a [Datastore](https://github.com/ipfs/go-datastore) for [IPLD](https://ipld.io) blocks that operates on ZIP files, with the addition of some utility methods to interact via [CID](https://github.com/ipfs/go-cid) rather than the native Datastore Key type.

Documentation available at https://godoc.org/github.com/rvagg/go-ds-zipcar

A JavaScript implementation is also available at [js-ds-zipcar](https://github.com/rvagg/js-ds-zipcar).

```go
var ds *zipcar.ZipDatastore
var node *dag.RawNode = dag.NewRawNode([]byte("random meaningless bytes"))

ds, err := zipcar.NewDatastore("example.zcar")
if err != nil {
    log.Fatal(err)
}
defer ds.Close()

// store a new block, creates a new file entry in the ZIP archive
ds.PutCid(node.Cid(), node.RawData())

// retrieve a block, reading from the ZIP archive
got, err := ds.GetCid(node.Cid())

fmt.Printf("Retrieved [%s] from zipcar with CID [%s]\n", got, node.Cid())
```

Will output:

```
Retrieved [random meaningless bytes] from zipcar with CID [bafkreihwkf6mtnjobdqrkiksr7qhp6tiiqywux64aylunbvmfhzeql2coa]
```

*example.zcar* is now a properly formatted ZIP archive:

```
$ unzip -l example.zcar
Archive:  example.zcar
Length      Date    Time    Name
---------  ---------- -----   ----
          24  00-00-1980 00:00   bafkreihwkf6mtnjobdqrkiksr7qhp6tiiqywux64aylunbvmfhzeql2coa
---------                     -------
          24                     1 file
```

## License and Copyright

Copyright 2019 Rod Vagg

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.