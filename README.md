Command line interface for HDFS.

```
$ hdfs.py --help
usage: hdfs.py [-h] {cat,cp,ls,mkdir,mv,put,rm} ...

Perform operations on HDFS.

positional arguments:
  {cat,cp,ls,mkdir,mv,put,rm}
                        list of HDFS operations
    cat                 Fetch all files that match the file pattern <src> and display their content on stdout.
    cp                  Copy files that match the file pattern <src> to a destination.
    ls                  List the contents that match the specified file pattern.
    mkdir               Create a directory in specified location.
    mv                  Rename a file or directory.
    put                 Copy files from the local file system into HDFS.
    rm                  Delete all files that match the specified file pattern.

optional arguments:
  -h, --help            show this help message and exit
```
