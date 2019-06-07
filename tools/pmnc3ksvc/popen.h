/*
#
# Pythomnic3k Project, http://www.pythomnic.org/
#
# Copyright (c) 2005-2008 Dmitry Dvoinikov <dmitry@targeted.org>
#
# This C++ code snippet was borrowed from the Green project
# http://www.targeted.org/green/ and stripped of all error handling.
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
#
*/

#ifndef pmnc3ksvc_popen_h
#define pmnc3ksvc_popen_h

#include "common.h"

//-----------------------------------------------------------------------------

class CIOPipe;

class CSatelliteProcess
{
private:
    string m_strCommandLine;
    class CSatelliteProcessImpl;
    shared_ptr<CSatelliteProcessImpl> m_shpoImpl;
    bool m_boolCompleted;
    unsigned long m_ulRetCode;
	unsigned long m_ulDestructorWaitMs;
public:
    CSatelliteProcess(const string& _rstrCommandLine, const unsigned long _ulDestructorWaitMs);
    unsigned long WaitForCompletion(const unsigned long _ulWaitMs = 60000);
    ~CSatelliteProcess(void);
public:
    shared_array<char> Read(unsigned long& _rulDataLength);
    shared_array<char> ReadErr(unsigned long& _rulDataLength);
    void Write(const char* _pcData, const unsigned long _ulDataLength);
};

//-----------------------------------------------------------------------------

#endif