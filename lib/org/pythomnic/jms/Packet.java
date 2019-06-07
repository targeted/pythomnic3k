/*
# Pythomnic3k Project, http://www.pythomnic.org/
#
# This module implements a descendant of Properties class which
# is capable of deserializing itself from a stream in a particular
# format:
#
# key1=base64(value1)\n
# key2=bas\n
#  e64(val\n
#  ue2)\n
# \n
#
# where keys are alnum bytes and values are utf-8 bytes wrapped in
# base64, lines can be folded, folded lines start with spaces, and
# the packet ends with empty line.
#
# Serializing goes to a lot more complicated format:
#
# prefixXXXX1XXXkey1=base64(value1)suffix\n
# prefixXXXX2XXXkey2=bassuffix\n
# prefixXXXX3XXX e64(valsuffix\n
# prefixXXXX4XXX ue2)suffix\n
# prefixXXXX5XXXsuffix\n
#
# where each line is decorated with fixed (random and irrelevant) prefix
# and suffix, and the payload in between is prefixed with hexadecimal value
# of running CRC32 value of the packet lines, in the above example
#
# XXXX1XXX = CRC32("key1=base64(value1)")
# XXXX2XXX = CRC32("key1=base64(value1)key2=bas")
# XXXX3XXX = CRC32("key1=base64(value1)key2=bas e64(val")
# XXXX4XXX = CRC32("key1=base64(value1)key2=bas e64(val ue2)")
# XXXX5XXX = CRC32("key1=base64(value1)key2=bas e64(val ue2)") = XXXX4XXX
#
# The same packet format is supported by protocol_jms.py
#
# Copyright (c) 2005-2009 Dmitry Dvoinikov <dmitry@targeted.org>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
*/

package org.pythomnic.jms;

import java.lang.*;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.zip.*;

public class Packet extends Properties
{

    /*
    Static factory method for reading packets from a stream.
    */
    public static Packet loadFromStream(BufferedReader _objStream) throws IOException
    {

        Packet pktResult = new Packet();

        String strPrevLine = null;
        String strLine = _objStream.readLine();

        while (strLine != null && !strLine.equals(""))
        {

            if (!strLine.startsWith(" ")) // regular line, start of a new key/value pair
            {
                if (strPrevLine != null)
                {
                    String[] arrstrKeyValue64 = strPrevLine.split("=", 2);
                    pktResult.setProperty(arrstrKeyValue64[0],
                                          Base64Encoder.decodeString(arrstrKeyValue64[1]));
                }
                strPrevLine = strLine;
            }
            else // folded line, continuation of the previous key/value pair
            {
                if (strPrevLine != null)
                {
                    strPrevLine += strLine.substring(1);
                }
                else
                {
                    throw new IOException("incorrect folding");
                }
            }

            strLine = _objStream.readLine();

        }

        if (strLine == null) // eof is not allowed at this moment
        {
            throw new IOException("unexpected eof");
        }

        if (strPrevLine != null)
        {
            String[] arrstrKeyValue64 = strPrevLine.split("=", 2);
            pktResult.setProperty(arrstrKeyValue64[0],
                                  Base64Encoder.decodeString(arrstrKeyValue64[1]));
        }

        return pktResult;

    }

    /*
    Public method for writing packet to the stream.
    */
    public void saveToStream(BufferedWriter _objStream, int _iWrapWidth, String _strBOL, String _strEOL) throws IOException
    {

        CRC32 objCRC32 = null;

        Iterator iterEntries = entrySet().iterator();
        while (iterEntries.hasNext())
        {

            Map.Entry entKeyValue = (Map.Entry)iterEntries.next();
            String strKey = (String)entKeyValue.getKey();
            String strValue = (String)entKeyValue.getValue();
            String strLine = strKey + "=" + Base64Encoder.encodeString(strValue);

            if (strLine.length() <= _iWrapWidth) // the whole value fits in a single line
            {
                if (strLine.length() > 0)
                {
                    objCRC32 = writeToStream(objCRC32, _objStream, strLine, _strBOL, _strEOL);
                }
                continue;
            }

            // the first line is written in full

            objCRC32 = writeToStream(objCRC32, _objStream, strLine.substring(0, _iWrapWidth), _strBOL, _strEOL);

            strLine = strLine.substring(_iWrapWidth);

            // the rest of the lines begin with a space and are one char shorter

            int iFoldChunkSize = _iWrapWidth - 1;
            int iFullFoldChunks = strLine.length() / iFoldChunkSize;
            int iLastFoldChunk = strLine.length() % iFoldChunkSize;

            for (int i = 0; i < iFullFoldChunks; ++i) // write full lines
            {
                objCRC32 = writeToStream(objCRC32, _objStream, 
                                         " " + strLine.substring(i * iFoldChunkSize, (i + 1) * iFoldChunkSize),
                                         _strBOL, _strEOL);
            }

            if (iLastFoldChunk > 0) // write the last line
            {
                objCRC32 = writeToStream(objCRC32, _objStream, 
                                         " " + strLine.substring(iFullFoldChunks * iFoldChunkSize),
                                         _strBOL, _strEOL);
            }

        }

        writeToStream(objCRC32, _objStream, "", _strBOL, _strEOL);
        _objStream.flush();

    }

    /*
    Private method for decorating output lines with checksums.
    */
    private CRC32 writeToStream(CRC32 _objCRC32, BufferedWriter _objStream, String _strLine, String _strBOL, String _strEOL) throws IOException
    {
        
        CRC32 objCRC32 = _objCRC32;
        if (objCRC32 == null)
        {
             objCRC32 = new CRC32();
        }

        if (!_strLine.equals(""))
        {
            objCRC32.update(_strLine.getBytes());
        }

        String strCRC32 = Long.toHexString(objCRC32.getValue()).toUpperCase();
        strCRC32 = "00000000".substring(strCRC32.length()) + strCRC32;

        if (!_strLine.equals(""))
        {
            _objStream.write(_strBOL + strCRC32 + _strLine + _strEOL);
        }
        else
        {
            _objStream.write(_strBOL + strCRC32 + _strEOL);
        }
        _objStream.newLine();

        return objCRC32;

    }

}
