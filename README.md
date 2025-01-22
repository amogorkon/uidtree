Bplustree
=========

.. image:: https://travis-ci.org/NicolasLM/bplustree.svg?branch=master
    :target: https://travis-ci.org/NicolasLM/bplustree
.. image:: https://coveralls.io/repos/github/NicolasLM/bplustree/badge.svg?branch=master
    :target: https://coveralls.io/github/NicolasLM/bplustree?branch=master

An on-disk B+tree for Python 3.

It feels like a dict, but stored on disk. When to use it?


This project is under development: the format of the file may change between
versions. Do not use as your primary source of data.


Keys and values
---------------

Keys must have a natural order and must be serializable to bytes. Some default
serializers for the most common types are provided. For example to index UUIDs:

.. code:: python

    >>> import uuid
    >>> from bplustree import BPlusTree, UUIDSerializer
    >>> tree = BPlusTree('/tmp/bplustree.db', serializer=UUIDSerializer(), key_size=16)
    >>> tree.insert(uuid.uuid1(), b'foo')
    >>> list(tree.keys())
    [UUID('48f2553c-de23-4d20-95bf-6972a89f3bc0')]

Values on the other hand are always bytes. They can be of arbitrary length,
the parameter ``value_size=128`` defines the upper bound of value sizes that
can be stored in the tree itself. Values exceeding this limit are stored in
overflow pages. Each overflowing value occupies at least a full page.


Concurrency
-----------

The tree is thread-safe, it follows the multiple readers/single writer pattern.

It is safe to:

- Share an instance of a ``BPlusTree`` between multiple threads

It is NOT safe to:

- Share an instance of a ``BPlusTree`` between multiple processes
- Create multiple instances of ``BPlusTree`` pointing to the same file

Durability
----------

A write-ahead log (WAL) is used to ensure that the data is safe. All changes
made to the tree are appended to the WAL and only merged into the tree in an
operation called a checkpoint, usually when the tree is closed. This approach
is heavily inspired by other databases like SQLite.

If tree doesn't get closed properly (power outage, process killed...) the WAL
file is merged the next time the tree is opened.

Performances
------------

Like any database, there are many knobs to finely tune the engine and get the
best performance out of it:

- ``order``, or branching factor, defines how many entries each node will hold
- ``page_size`` is the amount of bytes allocated to a node and the length of
  read and write operations. It is best to keep it close to the block size of
  the disk
- ``cache_size`` to keep frequently used nodes at hand. Big caches prevent the
  expensive operation of creating Python objects from raw pages but use more
  memory

Some advices to efficiently use the tree:

- Insert elements in ascending order if possible, prefer UUID v1 to UUID v4
- Insert in batch with ``tree.batch_insert(iterator)`` instead of using
  ``tree.insert()`` in a loop
- Let the tree iterate for you instead of using ``tree.get()`` in a loop
- Use ``tree.checkpoint()`` from time to time if you insert a lot, this will
  prevent the WAL from growing unbounded
- Use small keys and values, set their limit and overflow values accordingly
- Store the file and WAL on a fast disk
