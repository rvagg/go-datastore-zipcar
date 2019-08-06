package zipcar

import (
	"fmt"
	"log"

	dag "github.com/ipfs/go-merkledag"
)

func ExampleNewDatastore() {
	var ds *ZipDatastore
	var node *dag.RawNode = dag.NewRawNode([]byte("random meaningless bytes"))

	ds, err := NewDatastore("example.zcar")
	if err != nil {
		log.Fatal(err)
	}
	defer ds.Close()

	// store a new block, creates a new file entry in the ZIP archive
	ds.PutCid(node.Cid(), node.RawData())

	// retrieve a block, reading from the ZIP archive
	got, err := ds.GetCid(node.Cid())

	fmt.Printf("Retrieved [%s] from zipcar with CID [%s]\n", got, node.Cid())

	/*
		$ unzip -l example.zcar
		Archive:  example.zcar
		Length      Date    Time    Name
		---------  ---------- -----   ----
				 24  00-00-1980 00:00   bafkreihwkf6mtnjobdqrkiksr7qhp6tiiqywux64aylunbvmfhzeql2coa
		---------                     -------
				 24                     1 file
	*/

	// Output:
	// Retrieved [random meaningless bytes] from zipcar with CID [bafkreihwkf6mtnjobdqrkiksr7qhp6tiiqywux64aylunbvmfhzeql2coa]
}
