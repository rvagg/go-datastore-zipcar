package zipcar

import (
	"bytes"
	"os"
	"testing"

	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	format "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	mh "github.com/multiformats/go-multihash"
)

// similar to tests in https://github.com/ipfs/go-car/blob/master/car_test.go
// encode rnd1 bunch of different ipld blocks and make sure they come out right,
// not strictly essential since the ZipDatastore is really just rnd1 []byte store
// but we'll go for expected usage and process realistic data we can share
// between languages

var rnd1 = dag.NewRawNode([]byte("aaaa"))
var rnd2 = dag.NewRawNode([]byte("bbbb"))
var rnd3 = dag.NewRawNode([]byte("cccc"))
var rndz = dag.NewRawNode([]byte("zzzz"))

var pnd1 = &dag.ProtoNode{}
var pnd2 = &dag.ProtoNode{}
var pnd3 = &dag.ProtoNode{}

var cnd1 *cbor.Node
var cnd2 *cbor.Node
var cnd3 *cbor.Node

type cborTest struct {
	Str string
	I   int
	B   bool
}

func TestSetup(t *testing.T) {
	var err error

	pnd1.AddNodeLink("cat", rnd1)
	pnd2.AddNodeLink("first", pnd1)
	pnd2.AddNodeLink("dog", rnd2)
	pnd3.AddNodeLink("second", pnd2)
	pnd3.AddNodeLink("bear", rnd3)

	cbor.RegisterCborType(cborTest{})

	cnd1, err = cbor.WrapObject(cborTest{"foo", 100, false}, mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	cnd2, err = cbor.WrapObject(cborTest{"bar", -100, false}, mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	cnd3, err = cbor.WrapObject(cborTest{"baz", 0, true}, mh.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}

	os.Remove("test.zcar")
}

func TestBuildNew(t *testing.T) {
	var ds *ZipDatastore
	var err error

	ds, err = NewDatastore("test.zcar")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = ds.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	for _, raw := range []*dag.RawNode{rnd1, rnd2, rnd3} {
		err = ds.PutCid(raw.Cid(), raw.RawData())
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, nd := range []*dag.ProtoNode{pnd1, pnd2, pnd3} {
		buf, err := nd.Marshal()
		if err != nil {
			t.Fatal(err)
		}
		err = ds.PutCid(nd.Cid(), buf)
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, nd := range []*cbor.Node{cnd1, cnd2, cnd3} {
		err = ds.PutCid(nd.Cid(), nd.RawData())
		if err != nil {
			t.Fatal(err)
		}
	}

	// we are verifying from cache in this case
	verifyHasEntries(t, ds, false)
	verifyRawNodes(t, ds, false)
	verifyProtoNodes(t, ds, false)
	verifyCborNodes(t, ds, false)
}

func TestReadExisting(t *testing.T) {
	// in this case we are loading from scratch, no cache
	var ds *ZipDatastore
	var err error

	ds, err = NewDatastore("test.zcar")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = ds.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	verifyHasEntries(t, ds, false)
	verifyRawNodes(t, ds, false)
	verifyProtoNodes(t, ds, false)
	verifyCborNodes(t, ds, false)
}

func TestModifyExisting(t *testing.T) {
	// in this case we are loading from scratch, no cache
	var ds *ZipDatastore
	var err error

	ds, err = NewDatastore("test.zcar")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = ds.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	verifyHasEntries(t, ds, false)
	verifyRawNodes(t, ds, false)
	verifyProtoNodes(t, ds, false)
	verifyCborNodes(t, ds, false)

	err = ds.PutCid(rndz.Cid(), rndz.RawData())
	if err != nil {
		t.Fatal(err)
	}
	err = ds.DeleteCid(rnd2.Cid())
	if err != nil {
		t.Fatal(err)
	}
	err = ds.DeleteCid(pnd2.Cid())
	if err != nil {
		t.Fatal(err)
	}
	err = ds.DeleteCid(cnd2.Cid())
	if err != nil {
		t.Fatal(err)
	}

	verifyHasEntries(t, ds, true)
	verifyRawNodes(t, ds, true)
	verifyProtoNodes(t, ds, true)
	verifyCborNodes(t, ds, true)

	verifyHas(t, ds, rnd1.Cid(), "rnd1")
}

func TestReadModified(t *testing.T) {
	// in this case we are loading from scratch, no cache
	var ds *ZipDatastore
	var err error

	ds, err = NewDatastore("test.zcar")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = ds.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	verifyHasEntries(t, ds, true)
	verifyRawNodes(t, ds, true)
	verifyProtoNodes(t, ds, true)
	verifyCborNodes(t, ds, true)
	verifyHas(t, ds, rnd1.Cid(), "rnd1")
}

func TestTeardown(t *testing.T) {
	os.Remove("test.zcar")
}

func verifyHas(t *testing.T, ds *ZipDatastore, cid cid.Cid, name string) {
	var has bool
	var err error

	has, err = ds.HasCid(cid)
	if err != nil {
		t.Fatal(err)
	}
	if !has {
		t.Fatalf("%s not found in datastore", name)
	}
}

func verifyHasEntries(t *testing.T, ds *ZipDatastore, modified bool) {
	verifyHasnt := func(cid cid.Cid, name string) {
		var has bool
		var err error

		has, err = ds.HasCid(cid)
		if err != nil {
			t.Fatal(err)
		}
		if has {
			t.Fatalf("%s found in datastore, wut?", name)
		}
	}

	if modified {
		verifyHasnt(rnd2.Cid(), "rnd2")
		verifyHasnt(pnd2.Cid(), "pnd2")
		verifyHasnt(cnd2.Cid(), "cnd2")
	} else {
		verifyHas(t, ds, rnd2.Cid(), "rnd2")
		verifyHas(t, ds, pnd2.Cid(), "pnd2")
		verifyHas(t, ds, cnd2.Cid(), "cnd2")
	}

	verifyHas(t, ds, rnd1.Cid(), "rnd1")
	verifyHas(t, ds, rnd3.Cid(), "rnd3")
	verifyHas(t, ds, pnd1.Cid(), "pnd1")
	verifyHas(t, ds, pnd3.Cid(), "pnd3")
	verifyHas(t, ds, cnd1.Cid(), "cnd1")
	verifyHas(t, ds, cnd3.Cid(), "cnd3")

	verifyHasnt(dag.NewRawNode([]byte("dddd")).Cid(), "d")
}

func verifyRawNodes(t *testing.T, ds *ZipDatastore, modified bool) {
	for i, raw := range []*dag.RawNode{rnd1, rnd2, rnd3} {
		if modified && i == 1 {
			continue
		}
		data, err := ds.GetCid(raw.Cid())
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(data, raw.RawData()) != 0 {
			t.Errorf("data [%s] != original [%s]", data, raw.RawData())
		}

		size, err := ds.GetSizeCid(raw.Cid())
		if size != 4 {
			t.Fatal("Unexpected size of raw node's block")
		}
	}
}

func verifyProtoNodes(t *testing.T, ds *ZipDatastore, modified bool) {
	var data []byte
	var node *dag.ProtoNode
	var err error
	var link *format.Link

	// pnd1
	data, err = ds.GetCid(pnd1.Cid())
	if err != nil {
		t.Fatal(err)
	}
	node, err = dag.DecodeProtobuf(data)
	if err != nil {
		t.Fatal(err)
	}

	link, err = node.GetNodeLink("cat")
	if err != nil {
		t.Fatal(err)
	}
	if link.Cid != rnd1.Cid() {
		t.Errorf("Incorrect link in ProtoNode pnd1 / cat")
	}

	// pnd2
	if !modified {
		data, err = ds.GetCid(pnd2.Cid())
		if err != nil {
			t.Fatal(err)
		}
		node, err = dag.DecodeProtobuf(data)
		if err != nil {
			t.Fatal(err)
		}

		link, err = node.GetNodeLink("dog")
		if err != nil {
			t.Fatal(err)
		}
		if link.Cid != rnd2.Cid() {
			t.Errorf("Incorrect link in ProtoNode pnd2 / dog")
		}
		link, err = node.GetNodeLink("first")
		if err != nil {
			t.Fatal(err)
		}
		if link.Cid != pnd1.Cid() {
			t.Errorf("Incorrect link in ProtoNode pnd2 / first")
		}
	}

	// pnd3
	data, err = ds.GetCid(pnd3.Cid())
	if err != nil {
		t.Fatal(err)
	}
	node, err = dag.DecodeProtobuf(data)
	if err != nil {
		t.Fatal(err)
	}

	link, err = node.GetNodeLink("bear")
	if err != nil {
		t.Fatal(err)
	}
	if link.Cid != rnd3.Cid() {
		t.Errorf("Incorrect link in ProtoNode pnd3 / bear")
	}

	link, err = node.GetNodeLink("second")
	if err != nil {
		t.Fatal(err)
	}
	if link.Cid != pnd2.Cid() {
		t.Errorf("Incorrect link in ProtoNode pnd3 / second")
	}
}

func verifyCborNodes(t *testing.T, ds *ZipDatastore, modified bool) {
	var data []byte
	var err error

	// cnd1
	data, err = ds.GetCid(cnd1.Cid())
	if err != nil {
		t.Fatal(err)
	}
	cnd1A := cborTest{}
	err = cbor.DecodeInto(data, &cnd1A)
	if err != nil {
		t.Fatal(err)
	}

	if cnd1A.Str != "foo" {
		t.Fatal("cnd1 did not decode properly (str)")
	}
	if cnd1A.I != 100 {
		t.Fatal("cnd1 did not decode properly (i)")
	}
	if cnd1A.B {
		t.Fatal("cnd1 did not decode properly (B)")
	}

	if !modified {
		// cnd2
		data, err = ds.GetCid(cnd2.Cid())
		if err != nil {
			t.Fatal(err)
		}
		cnd2A := cborTest{}
		err = cbor.DecodeInto(data, &cnd2A)
		if err != nil {
			t.Fatal(err)
		}

		if cnd2A.Str != "bar" {
			t.Fatal("cnd2 did not decode properly (str)")
		}
		if cnd2A.I != -100 {
			t.Fatal("cnd2 did not decode properly (i)")
		}
		if cnd2A.B {
			t.Fatal("cnd2 did not decode properly (B)")
		}
	}

	// cnd3
	data, err = ds.GetCid(cnd3.Cid())
	if err != nil {
		t.Fatal(err)
	}
	cnd3A := cborTest{}
	err = cbor.DecodeInto(data, &cnd3A)
	if err != nil {
		t.Fatal(err)
	}

	if cnd3A.Str != "baz" {
		t.Fatal("cnd3 did not decode properly (str)")
	}
	if cnd3A.I != 0 {
		t.Fatal("cnd3 did not decode properly (i)")
	}
	if !cnd3A.B {
		t.Fatal("cnd3 did not decode properly (B)")
	}
}
