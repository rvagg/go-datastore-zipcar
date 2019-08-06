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
// encode a bunch of different ipld blocks and make sure they come out right,
// not strictly essential since the ZipDatastore is really just a []byte store
// but we'll go for expected usage and process realistic data we can share
// between languages

var a = dag.NewRawNode([]byte("aaaa"))
var b = dag.NewRawNode([]byte("bbbb"))
var c = dag.NewRawNode([]byte("cccc"))

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

	pnd1.AddNodeLink("cat", a)
	pnd2.AddNodeLink("first", pnd1)
	pnd2.AddNodeLink("dog", b)
	pnd3.AddNodeLink("second", pnd2)
	pnd3.AddNodeLink("bear", c)

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

func TestBuild(t *testing.T) {
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

	for _, raw := range []*dag.RawNode{a, b, c} {
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
	verifyHasEntries(t, ds)
	verifyRawNodes(t, ds)
	verifyProtoNodes(t, ds)
	verifyCborNodes(t, ds)
}

func TestRead(t *testing.T) {
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

	verifyHasEntries(t, ds)
	verifyRawNodes(t, ds)
	verifyProtoNodes(t, ds)
	verifyCborNodes(t, ds)
}

func TestTeardown(t *testing.T) {
	os.Remove("test.zcar")
}

func verifyHasEntries(t *testing.T, ds *ZipDatastore) {
	verifyHas := func(cid cid.Cid, name string) {
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

	verifyHas(a.Cid(), "a")
	verifyHas(b.Cid(), "b")
	verifyHas(c.Cid(), "c")
	verifyHas(pnd1.Cid(), "pnd1")
	verifyHas(pnd2.Cid(), "pnd2")
	verifyHas(pnd3.Cid(), "pnd3")
	verifyHas(cnd1.Cid(), "cnd1")
	verifyHas(cnd2.Cid(), "cnd2")
	verifyHas(cnd3.Cid(), "cnd3")

	verifyHasnt(dag.NewRawNode([]byte("dddd")).Cid(), "d")
}

func verifyRawNodes(t *testing.T, ds *ZipDatastore) {
	for _, raw := range []*dag.RawNode{a, b, c} {
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

func verifyProtoNodes(t *testing.T, ds *ZipDatastore) {
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
	if link.Cid != a.Cid() {
		t.Errorf("Incorrect link in ProtoNode pnd1 / cat")
	}

	// pnd2
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
	if link.Cid != b.Cid() {
		t.Errorf("Incorrect link in ProtoNode pnd2 / dog")
	}
	link, err = node.GetNodeLink("first")
	if err != nil {
		t.Fatal(err)
	}
	if link.Cid != pnd1.Cid() {
		t.Errorf("Incorrect link in ProtoNode pnd2 / first")
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
	if link.Cid != c.Cid() {
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

func verifyCborNodes(t *testing.T, ds *ZipDatastore) {
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
		t.Fatal("cnd1 did not decode properly (b)")
	}

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
		t.Fatal("cnd2 did not decode properly (b)")
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
		t.Fatal("cnd3 did not decode properly (b)")
	}
}
