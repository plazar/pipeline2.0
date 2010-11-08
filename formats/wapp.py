#!/usr/bin/env python

"""
wapp.py - WAPP file object

Define a WAPP file object that can:
    - Interpret the header parameters of the WAPP data.

Requires: pycparser - A parser for the C language written in python
(http://code.google.com/p/pycparser/)

Patrick Lazarus, Sept. 10, 2010
"""

import sys
import warnings
import os.path
import struct
import subprocess

import numpy as np
import pycparser


class wapp:
    def __init__(self, wappfn):
        if not os.path.isfile(wappfn):
            print "ERROR: File does not exist!\n\t(%s)" % wappfn
            self = None
            return
        else:
            self.filename = wappfn
            self.already_read_binary_header = False
            self.already_read_ascii_header = False
            self.already_parsed_ascii_header = False
            self.ascii_header = ""
            self.ascii_header_size = None # in bytes
            self.header_params = []
            self.header_types = {}
            self.header = {}
            self.file_size = os.path.getsize(self.filename)
            self.binary_header_size = None
            self.header_size = None
            self.data_size = None
            self.number_of_samples = None
            self.obs_time = None # Duration of data in file (in seconds)
            self.bytes_per_lag = None
            self.wappfile = open(self.filename, 'rb')
            self.read_header()
    
    def close(self):
        self.wappfile.close()

    def __del__(self):
        self.close()

    def read_header(self):
        if self.already_read_binary_header:
            return
        self.read_ascii_header()
        # Parse ASCII header to get WAPP_HEADER struct definition
        self.parse_ascii_header()
        # Now that we have parameter names and types, unpack 
        # binary data
        for name, charcode in zip(self.header_params, self.header_types):
            binarydata = struct.unpack(charcode, \
                            self.wappfile.read(struct.calcsize(charcode)))
            if charcode[-1] == 'c':
                chars = [c for c in binarydata if ord(c)] # Non-Null characters
                if chars:
                    self.header[name] = ''.join(chars)
            elif int(charcode[:-1]) == 1:
                self.header[name] = binarydata[0]
            else:
                self.header[name] = binarydata
        self.already_read_binary_header = True

        # Calculate some useful stuff
        self.header_size = self.wappfile.tell()
        self.binary_header_size = self.header_size - self.ascii_header_size
        self.data_size = self.file_size - self.header_size
        if self.header['lagformat']==0:
            # 16 bit lags, so 2 bytes per sample
            self.bytes_per_lag = 2.0
        elif self.heder['lagformat']==1:
            # 32 bit lags, so 4 bytes per sample
            self.bytes_per_lag = 4.0
        else:
            raise ValueError("Unexpected lagformat (%d)." % self.header['lagformat'])
        self.number_of_samples = self.data_size/self.bytes_per_lag / \
                                    self.header['num_lags']
        self.obs_time = self.header['samp_time']*1e-6*self.number_of_samples
        


    def read_ascii_header(self):
        """Peel off ASCII header from WAPP file and
            store the text in self.ascii_header.
        """
        if self.already_read_ascii_header:
            return
        self.seek_to_ascii_header_start()
        char = self.wappfile.read(1) # read first character
        while char != '\0':
            self.ascii_header += char
            char = self.wappfile.read(1) # read next character
        self.ascii_header_size = self.wappfile.tell()
        self.already_read_ascii_header = True

    def parse_ascii_header(self, use_cpp=True):
        """Parse the text from the WAPP file's ASCII header
            stored in self.ascii_header using the 'pycparser'
            module.

            By stepping through the parsed result from 'pycparse'
            fill self.header_params and self.header_types.

            If use_cpp is True, pass the WAPP file's ASCII
            header through the C Pre-processor before parsing.
            (Default is to use cpp.)
        """
        if self.already_parsed_ascii_header:
            return
        if use_cpp:
            # Pass ascii header through cpp
            cpp_pipe = subprocess.Popen("cpp", stdin=subprocess.PIPE, \
                            stdout=subprocess.PIPE, universal_newlines=True)
            pipeout, pipeerr = cpp_pipe.communicate(self.ascii_header)
            cpp_pipe.stdin.close()
            hdrtext = pipeout
        else:
            hdrtext = self.ascii_header

        parser = pycparser.c_parser.CParser()
        ast = parser.parse(hdrtext, filename=self.filename)
        # Step through the AST (abstract syntax tree) until
        # reaching the node that defines the struct 'WAPP_HEADER'
        wapp_struct = self.find_wapp_struct_node(ast)

        # Names of parameters are names of variable delarations
        self.header_params = [decl.name for decl in wapp_struct.decls]

        # Types of parameters need to be converted to struct modules
        # formatting codes
        self.header_types = [decl_to_charcode(decl) \
                                for decl in wapp_struct.decls]
        self.already_parsed_ascii_header = True

    def find_wapp_struct_node(self, node):
        """Given an AST node from a WAPP file's ASCII header
            find and return the node if it is a struct with
            name 'WAPP_HEADER' otherwise recursively search
            children.
        """
        if isinstance(node, pycparser.c_ast.Struct) and \
            node.name == 'WAPP_HEADER':
            return node
        for child in node.children():
            result = self.find_wapp_struct_node(child)
            if result is not None:
                return result
        return None

    def seek_to_ascii_header_start(self):
        """Seek to the start of the WAPP file's ASCII
            header (ie start of the WAPP file).
        """
        self.wappfile.seek(0)


def decl_to_charcode(decl):
    """Convert a declaration to a character code usable
        by python's 'struct' module.
    """
    if isinstance(decl.type, pycparser.c_ast.ArrayDecl):
        # Array
        arraydecl = decl.type
        size = int(arraydecl.dim.value)
        typedecl = arraydecl.type
    else:
        size = 1
        typedecl = decl.type

    # Convert type to character code
    ctype = sorted([x.lower() for x in typedecl.type.names])
    if sorted(['char']) == ctype:
        charcode = 'c'
    elif sorted(['signed', 'char']) == ctype:
        charcode = 'b'
    elif sorted(['unsigned', 'char']) == ctype:
        charcode = 'B'
    elif sorted(['_bool']) == ctype:
        charcode = '?'
    elif sorted(['short']) == ctype:
        charcode = 'h'
    elif sorted(['unsigned', 'short']) == ctype:
        charcode = 'H'
    elif sorted(['int']) == ctype:
        charcode = 'i'
    elif sorted(['unsigned', 'int']) == ctype:
        charcode = 'I'
    elif sorted(['long']) == ctype:
        charcode = 'l'
    elif sorted(['unsigned', 'long']) == ctype:
        charcode = 'L'
    elif sorted(['long', 'long']) == ctype:
        charcode = 'q'
    elif sorted(['unsigned', 'long', 'long']) == ctype:
        charcode = 'Q'
    elif sorted(['float']) == ctype:
        charcode = 'f'
    elif sorted(['double']) == ctype:
        charcode = 'd'
    else:
        raise ValueError("Unrecognized C type %s" % ctype)
    return "%d%s" % (size, charcode)
