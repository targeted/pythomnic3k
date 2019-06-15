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

#ifdef WIN32

#include "stdafx.h"

//-----------------------------------------------------------------------------

class CIOPipe
{
public:
    DWORD m_dwHandleType;
    HANDLE m_hSaved, m_hRead, m_hWrite, m_hClose;
public:
    CIOPipe(const DWORD _dwHandleType, const char _cMode);
    ~CIOPipe(void);
};

//-----------------------------------------------------------------------------

CIOPipe::CIOPipe(const DWORD _dwHandleType, const char _cMode)
: m_dwHandleType(_dwHandleType)
, m_hRead(INVALID_HANDLE_VALUE)
, m_hWrite(INVALID_HANDLE_VALUE)
{

    SECURITY_ATTRIBUTES strucSA;
    SECURITY_DESCRIPTOR strucSD;
    InitializeSecurityDescriptor(&strucSD, SECURITY_DESCRIPTOR_REVISION);
    SetSecurityDescriptorDacl(&strucSD, TRUE, NULL, FALSE);
    strucSA.nLength = sizeof(SECURITY_ATTRIBUTES);
    strucSA.bInheritHandle = TRUE;
    strucSA.lpSecurityDescriptor = &strucSD;

    CreatePipe(&m_hRead, &m_hWrite, &strucSA, 0);
    m_hSaved = GetStdHandle(_dwHandleType);
        
    if (_cMode == 'r')
    {

        HANDLE hRead;
        DuplicateHandle(GetCurrentProcess(), m_hRead, GetCurrentProcess(), &hRead, 
                        0, FALSE, DUPLICATE_SAME_ACCESS);
        CloseHandle(m_hRead);
        m_hRead = hRead;

        SetStdHandle(m_dwHandleType, m_hWrite);
        m_hClose = m_hWrite;

    }
    else if (_cMode == 'w')
    {
        
        HANDLE hWrite;
        DuplicateHandle(GetCurrentProcess(), m_hWrite, GetCurrentProcess(), &hWrite, 
                        0, FALSE, DUPLICATE_SAME_ACCESS);
        CloseHandle(m_hWrite);
        m_hWrite = hWrite;

        SetStdHandle(m_dwHandleType, m_hRead);
        m_hClose = m_hRead;

    }

}

//-----------------------------------------------------------------------------

CIOPipe::~CIOPipe(void)
{
    SetStdHandle(m_dwHandleType, m_hSaved);
    CloseHandle(m_hClose);
}

//-----------------------------------------------------------------------------

class CSatelliteProcess::CSatelliteProcessImpl
{
private:
    HANDLE m_hStdIn, m_hStdOut, m_hStdErr;
private:
    shared_array<char> ReadInternal(HANDLE _hFile, unsigned long& _rulDataLength);
public:
    HANDLE m_hProcess;
public:
    void Write(const char* _pcData, const unsigned long _ulDataLength);
    shared_array<char> Read(unsigned long& _rulDataLength);
    shared_array<char> ReadErr(unsigned long& _rulDataLength);
public:
    CSatelliteProcessImpl(const HANDLE _hStdIn, const HANDLE _hStdOut, const HANDLE _hStdErr, const HANDLE _hProcess)
        : m_hStdIn(_hStdIn), m_hStdOut(_hStdOut), m_hStdErr(_hStdErr), m_hProcess(_hProcess) {}
    ~CSatelliteProcessImpl(void) { CloseHandle(m_hStdIn); CloseHandle(m_hStdOut); CloseHandle(m_hStdErr); CloseHandle(m_hProcess); }
};

//-----------------------------------------------------------------------------

void CSatelliteProcess::CSatelliteProcessImpl::Write(const char* _pcData, const unsigned long _ulDataLength)
{

    unsigned long ulDataLength = _ulDataLength;
    if (ulDataLength > PIPE_OUTPUT_BUFFER_SIZE)
    {
        ulDataLength = PIPE_OUTPUT_BUFFER_SIZE;
    }

    DWORD dwWr;
    WriteFile(m_hStdIn, _pcData, ulDataLength, &dwWr, 0);

    FlushFileBuffers(m_hStdIn);

}

//-----------------------------------------------------------------------------

shared_array<char> CSatelliteProcess::CSatelliteProcessImpl::Read(unsigned long& _rulDataLength)
{
    return ReadInternal(m_hStdOut, _rulDataLength);
}

//-----------------------------------------------------------------------------

shared_array<char> CSatelliteProcess::CSatelliteProcessImpl::ReadErr(unsigned long& _rulDataLength)
{
    return ReadInternal(m_hStdErr, _rulDataLength);
}

//-----------------------------------------------------------------------------

shared_array<char> CSatelliteProcess::CSatelliteProcessImpl::ReadInternal(HANDLE _hFile, unsigned long& _rulDataLength)
{
    
    _rulDataLength = 0;

    unsigned long ulBufferSize = PIPE_INPUT_BUFFER_SIZE;
    shared_array<char> shparrcResult(new char[ulBufferSize]);

    DWORD dwRd;
    if (!ReadFile(_hFile, shparrcResult.get(), PIPE_INPUT_BUFFER_SIZE, &dwRd, 0) && 
        GetLastError() != ERROR_BROKEN_PIPE && GetLastError() != ERROR_NO_DATA)
    {
        dwRd = 0;
    }

    _rulDataLength = dwRd;
    return shparrcResult;

}

//-----------------------------------------------------------------------------

CSatelliteProcess::CSatelliteProcess(const string& _rstrCommandLine, const unsigned long _ulDestructorWaitMs)
: m_strCommandLine(_rstrCommandLine)
, m_boolCompleted(false)
, m_ulDestructorWaitMs(_ulDestructorWaitMs)
{

    // note - this constructor used to be thread-interlocked, but since
    // this service wrapper launches a single process once - this is no 
    // longer necessary

    CIOPipe oStdIn(STD_INPUT_HANDLE, 'w');
    CIOPipe oStdOut(STD_OUTPUT_HANDLE, 'r');
    CIOPipe oStdErr(STD_ERROR_HANDLE, 'r');

    PROCESS_INFORMATION strucPI; 
    memset(&strucPI, 0, sizeof(PROCESS_INFORMATION));

    STARTUPINFO strucSI;
    memset(&strucSI, 0, sizeof(STARTUPINFO));
    strucSI.cb = sizeof(STARTUPINFO); 
    strucSI.hStdInput = oStdIn.m_hRead;
    strucSI.hStdOutput = oStdOut.m_hWrite;
    strucSI.hStdError = oStdErr.m_hWrite;
    strucSI.wShowWindow = SW_HIDE;
    strucSI.dwFlags |= STARTF_USESTDHANDLES | STARTF_USESHOWWINDOW;

    HANDLE hProcessHandle;
    if (CreateProcess(0, const_cast<LPSTR>(m_strCommandLine.c_str()), 0, 0, 
                      TRUE, CREATE_NEW_CONSOLE, 0, 0, &strucSI, &strucPI))
    {
        CloseHandle(strucPI.hThread);
        hProcessHandle = strucPI.hProcess;
    }
    else
    {
        hProcessHandle = INVALID_HANDLE_VALUE;
    }

    m_shpoImpl.reset(new CSatelliteProcessImpl(oStdIn.m_hWrite, oStdOut.m_hRead, oStdErr.m_hRead, hProcessHandle));

}

//-----------------------------------------------------------------------------

unsigned long CSatelliteProcess::WaitForCompletion(const unsigned long _ulWaitMs)
{
    
    if (m_boolCompleted) return m_ulRetCode;

    if (WaitForSingleObject(m_shpoImpl->m_hProcess, _ulWaitMs) != WAIT_OBJECT_0)
    {
        TerminateProcess(m_shpoImpl->m_hProcess, 0);
    }

    DWORD dwExitCode = 0;
    GetExitCodeProcess(m_shpoImpl->m_hProcess, &dwExitCode);

    m_ulRetCode = dwExitCode;
    m_boolCompleted = true;

    return m_ulRetCode;

}

//-----------------------------------------------------------------------------

CSatelliteProcess::~CSatelliteProcess(void)
{
    if (!m_boolCompleted)
    {
        WaitForCompletion(m_ulDestructorWaitMs);
    }
}

//-----------------------------------------------------------------------------

shared_array<char> CSatelliteProcess::Read(unsigned long& _rulDataLength)
{
    return m_shpoImpl->Read(_rulDataLength);
}

//-----------------------------------------------------------------------------

shared_array<char> CSatelliteProcess::ReadErr(unsigned long& _rulDataLength)
{
    return m_shpoImpl->ReadErr(_rulDataLength);
}

//-----------------------------------------------------------------------------

void CSatelliteProcess::Write(const char* _pcData, const unsigned long _ulDataLength)
{
    m_shpoImpl->Write(_pcData, _ulDataLength);
}

//-----------------------------------------------------------------------------

#endif
