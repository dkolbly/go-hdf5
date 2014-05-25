package hdf5

// most of the hdf5 is oriented around static knowledge
// of types in HDF5 files; the functions here are to support
// dynamically typed operations


// #include "hdf5.h"
// #include "H5PTpublic.h"
// #include <stdlib.h>
// static inline int _go_hdf5_sizeof_uintptr() { return sizeof(void*); }
import "C"

import (
	"fmt"
	"runtime"
	"unsafe"
	"reflect"
)

type hdfdynamicerror struct {
	code int
}

func (h *hdfdynamicerror) Error() string {
	return fmt.Sprintf("**hdf5 dynamic access error** code=%d", h.code)
}

func dynamic_error(code int) error {
	return &hdfdynamicerror{code: code}
}

type DynDatatype struct {
	Location
	class		TypeClass
}

// Size returns the size of the Datatype.
func (t *DynDatatype) Size() uint {
	return uint(C.H5Tget_size(t.id))
}

// Class returns the class of the Datatype.
func (t *DynDatatype) Class() TypeClass {
	return t.class
}

func (t *DynDatatype) finalizer() {
	err := t.Close()
	if err != nil {
		panic(fmt.Sprintf("error closing datatype: %s", err))
	}
}

func (t *DynDatatype) Close() error {
	if t.id > 0 {
		err := h5err(C.H5Tclose(t.id))
		t.id = 0
		return err
	}
	return nil
}

func (t *DynDatatype) String() string {
	return fmt.Sprintf("<hdf5.DynDatatype %d %d>",
		t.id,
		t.class)
}

func NewDynDatatype(id C.hid_t) *DynDatatype {
	cls := TypeClass(C.H5Tget_class(id))
	t := &DynDatatype{Location{id}, cls}
	runtime.SetFinalizer(t, (*DynDatatype).finalizer)
	return t
}

func (t *DynDatatype) NMembers() (int, error) {
	if t.class != T_COMPOUND {
		return 0, dynamic_error(7301)
	}
	return int(C.H5Tget_nmembers(t.id)), nil
}

// Returns the datatype of the specified member.
func (t *DynDatatype) MemberType(mbr_idx int) (*DynDatatype, error) {
	if t.class != T_COMPOUND {
		return nil, dynamic_error(7302)
	}
	cls := C.H5Tget_member_class(t.id, C.uint(mbr_idx))
	err := h5err(C.herr_t(int(cls)))
	if err != nil {
		return nil, err
	}
	hid := C.H5Tget_member_type(t.id, C.uint(mbr_idx))
	err = h5err(C.herr_t(int(hid)))
	if err != nil {
		return nil, err
	}
	return NewDynDatatype(hid), nil
}

// Returns the name of the specified member.
func (t *DynDatatype) MemberName(mbr_idx int) string {
	c_name := C.H5Tget_member_name(t.id, C.uint(mbr_idx))
	return C.GoString(c_name)
}

// Returns the offset of the specified member.
func (t *DynDatatype) MemberOffset(mbr_idx int) int {
	k := C.H5Tget_member_offset(t.id, C.uint(mbr_idx))
	return int(k)
}

// Returns a map of members (convenience function)
func (t *DynDatatype) Members() (map[string]*DynDatatype, error) {
	if t.class != T_COMPOUND {
		return nil, dynamic_error(7303)
	}
	n, _ := t.NMembers()
	m := make(map[string]*DynDatatype, n)
	for i := 0; i < n; i++ {
		mtype, err := t.MemberType(i)
		if err == nil {
			m[t.MemberName(i)] = mtype
		}
	}
	return m, nil
}


func (t *Datatype) AsDynamic() *DynDatatype {
	hid := C.H5Tcopy(t.id)
	if hid < 0 {
		return nil
	}
	return NewDynDatatype(hid)
}

type TableReader struct {
	source	 	*Table
	bytesPerRecord	int
	buffer		unsafe.Pointer
	bufferCap	int
	remain		int64
}

func (rdr *TableReader) finalizer() {
	C.free(rdr.buffer)
}

type DynPacket struct {
	Data		[]byte
}

// Returns a slice of DynPacket objects, each of which
// contains a byte slice.  Reuses the underlying storage
// associated with the TableReader.  Will return fewer
// objects if the num of them don't fit in the buffer, or
// there are fewer left.  Returns nil at EOF (but no
// error is indicated)

func (rdr *TableReader) Read(num int) ([]DynPacket, error) {
	if num * rdr.bytesPerRecord > rdr.bufferCap {
		num = rdr.bufferCap / rdr.bytesPerRecord
	}
	if int64(num) > rdr.remain {
		num = int(rdr.remain)
	}
	if num == 0 {
		// EOF
		return nil, nil
	}
	rc := C.H5PTget_next(rdr.source.id, C.size_t(num), rdr.buffer)
	if rc < 0 {
		return nil, h5err(rc)
	}
	rdr.remain -= int64(num)
	vec := make([]DynPacket, num)
	var p uintptr = uintptr(rdr.buffer)
	for i := 0; i < num; i++ {
		hdr := reflect.SliceHeader{
			Data: p,
			Len: rdr.bytesPerRecord,
			Cap: rdr.bytesPerRecord,
		}
		vec[i] = DynPacket{
			Data: *(*[]byte)(unsafe.Pointer(&hdr)),
			}
		p += uintptr(rdr.bytesPerRecord)
	}
	return vec, nil
}

func (t *Table) MakeTableReader() (*TableReader, error) {
	err := t.CreateIndex()
	if err != nil {
		return nil, err
	}
	c_nrecords := C.hsize_t(0)
	rc := C.H5PTget_num_packets(t.id, &c_nrecords)
	if rc < 0 {
		return nil, h5err(rc)
	}

	bcap := 1000000
	var buf unsafe.Pointer = C.malloc(C.size_t(bcap))

	per := 32
	rdr := &TableReader{
		source: t, 
		bytesPerRecord: per, 
		buffer: buf, 
		bufferCap: bcap,
		remain: int64(c_nrecords)}
	runtime.SetFinalizer(rdr, (*TableReader).finalizer)
	return rdr, nil
}

func (t *Table) ReadPacketBytes(start int, count int) []byte {
	per := 24
	var buf unsafe.Pointer = C.malloc(1000000)
	nbytes := count * per

	rc := C.H5PTread_packets(t.id, C.hsize_t(start), C.size_t(count), buf)
	if rc < 0 {
		C.free(buf)
		return nil
	}
	slice := (*[1<<30]byte)(unsafe.Pointer(buf))[:nbytes:nbytes]
	return slice
}

type primitiveUnpack func (p uintptr) (interface{}, uintptr)

func integerUnpacker(p uintptr) (interface{}, uintptr) {
	var tmp int32 = *(*int32)(unsafe.Pointer(p))
	return tmp, p + 8
}

func longUnpacker(p uintptr) (interface{}, uintptr) {
	var tmp int64 = *(*int64)(unsafe.Pointer(p))
	return tmp, p + 8
}

func floatUnpacker(p uintptr) (interface{}, uintptr) {
	var tmp float32 = *(*float32)(unsafe.Pointer(p))
	return tmp, p + 4
}

func stringUnpacker(p uintptr) (interface{}, uintptr) {
	// god knows what we're leaking...
	var c_str uintptr = *(*uintptr)(unsafe.Pointer(p))
	nextp := p + uintptr(C._go_hdf5_sizeof_uintptr())
	if c_str == 0 {
		return "", nextp
	} else {
		return C.GoString((*C.char)(unsafe.Pointer(c_str))), nextp
	}
}

type Unpacker struct {
	unpackers	[]primitiveUnpack
}

func (t *DynDatatype) equal(b *Datatype) bool {
	v := int(C.H5Tequal(t.id, b.id))
	if v > 0 {
		return true
	}
	return false
}
	

func (t *DynDatatype) primitiveUnpacker() (primitiveUnpack, error) {
	switch {
	case t.equal(T_NATIVE_LLONG):
		return longUnpacker, nil
	case t.equal(T_NATIVE_INT):
		return integerUnpacker, nil
	case t.equal(T_NATIVE_FLOAT):
		return floatUnpacker, nil
	case t.equal(T_GO_STRING):
		return stringUnpacker, nil
	default:
		// unsupported type for unpacking
		return nil, dynamic_error(7307)
	}
}

func (t *DynDatatype) MakeUnpacker() (*Unpacker, error) {
	if t.class != T_COMPOUND {
		return nil, dynamic_error(7304)
	}
	n, _ := t.NMembers()
	u := make([]primitiveUnpack, n)
	for i := 0; i < n; i++ {
		mt, err := t.MemberType(i)
		if err != nil {
			return nil, err
		}
		
		ufunc, _ := mt.primitiveUnpacker()
		/*if err != nil {
			return nil, err
		}*/
		u[i] = ufunc
	}
	return &Unpacker{u}, nil
}

func (u *Unpacker) Unpack(data []byte) []interface{} {
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	var p uintptr = hdr.Data
	n := len(u.unpackers)
	result := make([]interface{}, n)
	for i := 0; i < n; i++ {
		if u.unpackers[i] != nil {
			result[i], p = u.unpackers[i](p)
		}
	}
	return result
}
