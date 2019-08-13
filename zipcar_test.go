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
	"github.com/stretchr/testify/assert"
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
	S string
	I int
	B bool
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
	assert.NoError(t, err)
	cnd2, err = cbor.WrapObject(cborTest{"bar", -100, false}, mh.SHA2_256, -1)
	assert.NoError(t, err)
	cnd3, err = cbor.WrapObject(cborTest{"baz", 0, true}, mh.SHA2_256, -1)
	assert.NoError(t, err)

	os.Remove("test.zcar")
}

func TestBuildNew(t *testing.T) {
	var ds *ZipDatastore
	var err error

	ds, err = NewDatastore("test.zcar")
	assert.NoError(t, err)

	defer func() {
		err = ds.Close()
		assert.NoError(t, err)
	}()

	for _, raw := range []*dag.RawNode{rnd1, rnd2, rnd3} {
		err = ds.PutCid(raw.Cid(), raw.RawData())
		assert.NoError(t, err)
	}

	for _, nd := range []*dag.ProtoNode{pnd1, pnd2, pnd3} {
		buf, err := nd.Marshal()
		assert.NoError(t, err)
		err = ds.PutCid(nd.Cid(), buf)
		assert.NoError(t, err)
	}

	for _, nd := range []*cbor.Node{cnd1, cnd2, cnd3} {
		err = ds.PutCid(nd.Cid(), nd.RawData())
		assert.NoError(t, err)
	}

	ds.SetComment(cnd3.Cid().String())

	// we are verifying from cache in this case
	verifyHasEntries(t, ds, false)
	verifyRawNodes(t, ds, false)
	verifyProtoNodes(t, ds, false)
	verifyCborNodes(t, ds, false)
	verifyComment(t, ds, false)
}

func TestReadExisting(t *testing.T) {
	// in this case we are loading from scratch, no cache
	var ds *ZipDatastore
	var err error

	ds, err = NewDatastore("test.zcar")
	assert.NoError(t, err)

	defer func() {
		err = ds.Close()
		assert.NoError(t, err)
	}()

	verifyHasEntries(t, ds, false)
	verifyRawNodes(t, ds, false)
	verifyProtoNodes(t, ds, false)
	verifyCborNodes(t, ds, false)
	verifyComment(t, ds, false)
}

func TestModifyExisting(t *testing.T) {
	// in this case we are loading from scratch, no cache
	var ds *ZipDatastore
	var err error

	ds, err = NewDatastore("test.zcar")
	assert.NoError(t, err)

	defer func() {
		err = ds.Close()
		assert.NoError(t, err)
	}()

	verifyHasEntries(t, ds, false)
	verifyRawNodes(t, ds, false)
	verifyProtoNodes(t, ds, false)
	verifyCborNodes(t, ds, false)
	verifyComment(t, ds, false)

	err = ds.PutCid(rndz.Cid(), rndz.RawData())
	assert.NoError(t, err)
	err = ds.DeleteCid(rnd2.Cid())
	assert.NoError(t, err)
	err = ds.DeleteCid(pnd2.Cid())
	assert.NoError(t, err)
	err = ds.DeleteCid(cnd2.Cid())
	assert.NoError(t, err)
	ds.SetComment(cnd2.Cid().String())

	verifyHasEntries(t, ds, true)
	verifyRawNodes(t, ds, true)
	verifyProtoNodes(t, ds, true)
	verifyCborNodes(t, ds, true)
	verifyComment(t, ds, true)
}

func TestReadModified(t *testing.T) {
	// in this case we are loading from scratch, no cache
	var ds *ZipDatastore
	var err error

	ds, err = NewDatastore("test.zcar")
	assert.NoError(t, err)

	defer func() {
		err = ds.Close()
		assert.NoError(t, err)
	}()

	verifyHasEntries(t, ds, true)
	verifyRawNodes(t, ds, true)
	verifyProtoNodes(t, ds, true)
	verifyCborNodes(t, ds, true)
	verifyComment(t, ds, true)
	verifyHas(t, ds, rnd1.Cid(), "rnd1")
}

func TestReadJS(t *testing.T) {
	// in this case we are loading a file created from JavaScript with the same data
	var ds *ZipDatastore
	var err error

	ds, err = NewDatastore("js.zcar")
	assert.NoError(t, err)

	defer func() {
		err = ds.Close()
		assert.NoError(t, err)
	}()

	verifyHasEntries(t, ds, false)
	verifyRawNodes(t, ds, false)
	verifyProtoNodes(t, ds, false)
	verifyCborNodes(t, ds, false)
	verifyComment(t, ds, false)
}

func TestTeardown(t *testing.T) {
	os.Remove("test.zcar")
}

func verifyHas(t *testing.T, ds *ZipDatastore, cid cid.Cid, name string) {
	var has bool
	var err error

	has, err = ds.HasCid(cid)
	assert.NoError(t, err)
	assert.Truef(t, has, "%s not found in datastore", name)
}

func verifyHasEntries(t *testing.T, ds *ZipDatastore, modified bool) {
	verifyHasnt := func(cid cid.Cid, name string) {
		var has bool
		var err error

		has, err = ds.HasCid(cid)
		assert.NoError(t, err)
		assert.Falsef(t, has, "%s found in datastore, wut?", name)
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
	for i, raw := range []*dag.RawNode{rnd1, rnd2, rnd3, rndz} {
		if modified && i == 1 {
			continue
		}
		if !modified && i == 3 {
			continue
		}
		data, err := ds.GetCid(raw.Cid())
		assert.NoError(t, err)
		if bytes.Compare(data, raw.RawData()) != 0 {
			t.Errorf("data [%s] != original [%s]", data, raw.RawData())
		}

		size, err := ds.GetSizeCid(raw.Cid())
		assert.Equal(t, 4, size, "Unexpected size of raw node's block")
	}
}

func verifyProtoNodes(t *testing.T, ds *ZipDatastore, modified bool) {
	var data []byte
	var node *dag.ProtoNode
	var err error
	var link *format.Link

	// pnd1
	data, err = ds.GetCid(pnd1.Cid())
	assert.NoError(t, err)
	node, err = dag.DecodeProtobuf(data)
	assert.NoError(t, err)

	link, err = node.GetNodeLink("cat")
	assert.NoError(t, err)
	if link.Cid != rnd1.Cid() {
		t.Errorf("Incorrect link in ProtoNode pnd1 / cat")
	}

	// pnd2
	if !modified {
		data, err = ds.GetCid(pnd2.Cid())
		assert.NoError(t, err)
		node, err = dag.DecodeProtobuf(data)
		assert.NoError(t, err)

		link, err = node.GetNodeLink("dog")
		assert.NoError(t, err)
		if link.Cid != rnd2.Cid() {
			t.Errorf("Incorrect link in ProtoNode pnd2 / dog")
		}
		link, err = node.GetNodeLink("first")
		assert.NoError(t, err)
		if link.Cid != pnd1.Cid() {
			t.Errorf("Incorrect link in ProtoNode pnd2 / first")
		}
	}

	// pnd3
	data, err = ds.GetCid(pnd3.Cid())
	assert.NoError(t, err)
	node, err = dag.DecodeProtobuf(data)
	assert.NoError(t, err)

	link, err = node.GetNodeLink("bear")
	assert.NoError(t, err)
	if link.Cid != rnd3.Cid() {
		t.Errorf("Incorrect link in ProtoNode pnd3 / bear")
	}

	link, err = node.GetNodeLink("second")
	assert.NoError(t, err)
	if link.Cid != pnd2.Cid() {
		t.Errorf("Incorrect link in ProtoNode pnd3 / second")
	}
}

func verifyCborNodes(t *testing.T, ds *ZipDatastore, modified bool) {
	var data []byte
	var err error

	// cnd1
	data, err = ds.GetCid(cnd1.Cid())
	assert.NoError(t, err)
	cnd1A := cborTest{}
	err = cbor.DecodeInto(data, &cnd1A)
	assert.NoError(t, err)

	assert.Equal(t, "foo", cnd1A.S, "cnd1 did not decode properly (str)")
	assert.Equal(t, 100, cnd1A.I, "cnd1 did not decode properly (i)")
	assert.Equal(t, false, cnd1A.B, "cnd1 did not decode properly (b)")

	if !modified {
		// cnd2
		data, err = ds.GetCid(cnd2.Cid())
		assert.NoError(t, err)
		cnd2A := cborTest{}
		err = cbor.DecodeInto(data, &cnd2A)
		assert.NoError(t, err)

		assert.Equal(t, "bar", cnd2A.S, "cnd2 did not decode properly (str)")
		assert.Equal(t, -100, cnd2A.I, "cnd2 did not decode properly (i)")
		assert.Equal(t, false, cnd2A.B, "cnd2 did not decode properly (b)")
	}

	// cnd3
	data, err = ds.GetCid(cnd3.Cid())
	assert.NoError(t, err)
	cnd3A := cborTest{}
	err = cbor.DecodeInto(data, &cnd3A)
	assert.NoError(t, err)

	assert.Equal(t, "baz", cnd3A.S, "cnd3 did not decode properly (str)")
	assert.Equal(t, 0, cnd3A.I, "cnd3 did not decode properly (i)")
	assert.Equal(t, true, cnd3A.B, "cnd3 did not decode properly (b)")
}

func verifyComment(t *testing.T, ds *ZipDatastore, modified bool) {
	var expected string

	if modified {
		expected = cnd2.String()
	} else {
		expected = cnd3.String()
	}

	assert.Equal(t, ds.Comment(), expected, "Unexpected archive comment")
}
