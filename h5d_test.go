package hdf5

import (
	"os"
	"testing"
)

func createDataset1(t *testing.T) error {
    // create a file with a single 5x20 dataset
    f, err := CreateFile(FNAME, F_ACC_TRUNC)
    if err != nil {
        t.Fatalf("CreateFile failed: %s", err)
        return err
    }
    defer f.Close()

    var data [100]uint16
    for i := range data {
        data[i] = uint16(i)
    }

    dims := []uint{20,5}
    dspace, err := CreateSimpleDataspace(dims, dims)
    if err != nil {
        t.Fatal(err)
        return err
    }

    dset, err := f.CreateDataset("dset", T_NATIVE_USHORT, dspace)
    if err != nil {
        t.Fatal(err)
        return err
    }

    err = dset.Write(&data[0], T_NATIVE_USHORT)
    if err != nil {
       t.Fatal(err)
       return err
    }
    return err
}

/**
 * TestSubset based on the h5_subset.c sample with the HDF5 C library.
 * Original copyright notice:
 *
 * HDF5 (Hierarchical Data Format 5) Software Library and Utilities
 * Copyright 2006-2013 by The HDF Group.
 *
 * NCSA HDF5 (Hierarchical Data Format 5) Software Library and Utilities
 * Copyright 1998-2006 by the Board of Trustees of the University of Illinois.
 ****
 * Write some test data then read back a subset.
 */
func TestSubset(t *testing.T) {
    DisplayErrors(true)
    defer DisplayErrors(false)
    defer os.Remove(FNAME)
    err := createDataset1(t)
    if err != nil {
        return
    }

    // load a subset of the data
    f, err := OpenFile(FNAME, F_ACC_RDONLY)
    if err != nil {
        t.Fatal(err)
    }
    defer f.Close()

    dset, err := f.OpenDataset("dset")
    if err != nil {
        t.Fatal(err)
    }

    // get the filespace and select the subset
    filespace := dset.Space()
    offset,stride,count,block := [2]uint{5,1}, [2]uint{1,1}, [2]uint{5,2}, [2]uint{1,1}
    err = filespace.SelectHyperslab(offset[:], stride[:], count[:], block[:]);
    if err != nil {
        t.Fatal(err)
    }

    // create the memory space for the subset
    dims, maxdims := [2]uint{2,5}, [2]uint{2,5}
    if err != nil {
        t.Fatal(err)
    }
    memspace,err := CreateSimpleDataspace(dims[:], maxdims[:])
    if err != nil {
        t.Fatal(err)
    }
    // create a buffer for the data
    var data [10]uint16

    // read the subset
    err = dset.ReadSubset(&data[0], T_NATIVE_USHORT, memspace, filespace)
    if err != nil {
        t.Fatal(err)
    }
    expected := [10]uint16{26,27,31,32,36,37,41,42,46,47}
    if data != expected {
        t.Fatal("Loaded data does not match expected.",data,expected);
    }
}
