/*
# Pythomnic3k Project, http://www.pythomnic.org/
#
# This module implements a set of static methods
# for encoding/decoding BASE64 data.
#
# Copyright (c) 2005-2008 Dmitry Dvoinikov <dmitry@targeted.org>
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

import java.io.*;

public class Base64Encoder
{

    private static char[] forward_map = new char[64];
    private static byte[] reverse_map = new byte[128];

    static
    {

        int i = 0; char ch;

        for (ch = 'A'; ch <= 'Z'; ch++) forward_map[i++] = ch;
        for (ch = 'a'; ch <= 'z'; ch++) forward_map[i++] = ch;
        for (ch = '0'; ch <= '9'; ch++) forward_map[i++] = ch;
        forward_map[i++] = '+';
        forward_map[i++] = '/';

        for (i = 0; i < reverse_map.length; i++) reverse_map[i] = -1;
        for (i = 0; i < 64; i++) reverse_map[forward_map[i]] = (byte)i;

    }

    public static char[] encode(byte[] source)
    {

        int source_length = source.length;
        int result_length_raw = (source_length * 4 + 2) / 3;
        int result_length = ((source_length + 2) / 3) * 4;

        char[] result = new char[result_length];
        int input_offset = 0;
        int output_offset = 0;

        while (input_offset < source_length)
        {

            int source_byte_0 = source[input_offset++] & 0xff;
            int source_byte_1 = input_offset < source_length ? source[input_offset++] & 0xff : 0;
            int source_byte_2 = input_offset < source_length ? source[input_offset++] & 0xff : 0;

            int result_byte_0 = source_byte_0 >>> 2;
            int result_byte_1 = ((source_byte_0 & 0x03) << 4) | (source_byte_1 >>> 4);
            int result_byte_2 = ((source_byte_1 & 0x0f) << 2) | (source_byte_2 >>> 6);
            int result_byte_3 = source_byte_2 & 0x3f;

            result[output_offset++] = forward_map[result_byte_0];
            result[output_offset++] = forward_map[result_byte_1];
            result[output_offset] = output_offset < result_length_raw ? forward_map[result_byte_2] : '=';
            output_offset++;
            result[output_offset] = output_offset < result_length_raw ? forward_map[result_byte_3] : '=';
            output_offset++;

        }

        return result;

    }

    public static byte[] decode(char[] source)
    {

        int source_length = source.length;
        if (source_length % 4 != 0)
        {
            throw new IllegalArgumentException("Invalid BASE64 encoded string length");
        }

        while (source_length > 0 && source[source_length - 1] == '=')
        {
            source_length -= 1;
        }

        int result_length = (source_length * 3) / 4;
        byte[] result = new byte[result_length];
        int input_offset = 0;
        int output_offset = 0;

        while (input_offset < source_length)
        {

            int source_byte_0 = source[input_offset++];
            int source_byte_1 = source[input_offset++];
            int source_byte_2 = input_offset < source_length ? source[input_offset++] : 'A';
            int source_byte_3 = input_offset < source_length ? source[input_offset++] : 'A';

            if (source_byte_0 > 127 || source_byte_1 > 127 ||
                source_byte_2 > 127 || source_byte_3 > 127)
            {
                throw new IllegalArgumentException("Invalid BASE64 encoded string");
            }

            int source_byte_raw_0 = reverse_map[source_byte_0];
            int source_byte_raw_1 = reverse_map[source_byte_1];
            int source_byte_raw_2 = reverse_map[source_byte_2];
            int source_byte_raw_3 = reverse_map[source_byte_3];

            if (source_byte_raw_0 < 0 || source_byte_raw_1 < 0 ||
                source_byte_raw_2 < 0 || source_byte_raw_3 < 0)
            {
                throw new IllegalArgumentException("Invalid BASE64 encoded string");
            }

            int result_byte_0 = (source_byte_raw_0 << 2) | (source_byte_raw_1 >>> 4);
            int result_byte_1 = ((source_byte_raw_1 & 0x0f) << 4) | (source_byte_raw_2 >>> 2);
            int result_byte_2 = ((source_byte_raw_2 & 0x03) << 6) | source_byte_raw_3;

            result[output_offset++] = (byte)result_byte_0;
            if (output_offset < result_length) result[output_offset++] = (byte)result_byte_1;
            if (output_offset < result_length) result[output_offset++] = (byte)result_byte_2;

        }

        return result;

    }

    /*
    Encodes a string into UTF-8 then wraps into BASE64.
    */
    public static String encodeString(String _strValue) throws UnsupportedEncodingException
    {
        if (_strValue != null)
        {
            byte[] arrbValue = _strValue.getBytes("UTF-8");
            char[] arrcValue = Base64Encoder.encode(arrbValue);
            return new String(arrcValue);
        }
        else
        {
            return "";
        }
    }

    /*
    Unwraps a string from BASE64 then decodes from UTF-8.
    */
    public static String decodeString(String _strValueBase64) throws UnsupportedEncodingException
    {
        if (_strValueBase64.length() > 0)
        {
            char[] arrcValue = _strValueBase64.toCharArray();
            byte[] arrbValue = Base64Encoder.decode(arrcValue);
            return new String(arrbValue, "UTF-8");
        }
        else
        {
            return "";
        }
    }

}