# AI Query

* Current code. Python 3.12 only, no backwards compatibility.
* This is a bplustree implementation that I am completely rewriting. I want to build a triplestore on top of it, so I need this bplustree implementation to be 100% specialized and optimized for that use case - no other use cases. This means that all keys and values will be uuid (each split into two int64) and hdf5 will be used for storage. I will post 4 files. Understand and acknowledge only, don't suggest anything.
* *post tree.py*
* *post node.py*
* *post entry.py*
* *post memory.py*
* `Now that I have posted and the code, let's start. Here is what I need help with:`


# Required Code Structure

* The name of the file needs to be on the first line of the file as a comment like `# filename.py`
so it can be easily copyed and pasted into a query.
* Alternatively, we could use a module docstring with the filename in it. This would also be compatible with a hashbang.
```
#!/usr/bin/env python3
"""
pyping.py - This file does XYZ...
"""
```
* Each file must be less than 10000 characters (about 300 lines) because queries are limited.
* As the AI can figure out the logic of the code, line comments are not needed, and may even be distracting and counterproductive. Comments also take up valuable characters in the query, so they should be avoided.
