/*
#
# Pythomnic3k Project, http://www.pythomnic.org/
#
# Copyright (c) 2005-2010 Dmitry Dvoinikov <dmitry@targeted.org>
#
# This C++ code snippet was borrowed from the Green project
# http://www.targeted.org/green/ and stripped of all Green specifics 
# and error handling.
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

#include "stdafx.h"

#define SERVICE_SHORT_NAME_PREFIX "pmnc3k_"
#define SERVICE_FULL_NAME_PREFIX "Pythomnic3k cage "
#define SERVICE_DESCRIPTION_PREFIX "Pythomnic3k-based service running cage "

#define MSG(X) MessageBox(0, (X), "pmnc3ksvc.exe", MB_OK)

//-------------------------------------------------------------------------------

bool split(const string& _rstrSrc, const string& _rstrSep, string& _rstrL, string& _rstrR)
{

    if (_rstrSrc.empty())
    {
        _rstrL.clear();
        _rstrR.clear();
        return false;
    }
        
    string strSrc = _rstrSrc;
    string strSep = _rstrSep;

    string::size_type iSepPos = strSrc.find(strSep);

    if (iSepPos == string::npos)
    {
        _rstrL = strSrc;
        _rstrR.clear();
        return false;
    }
    else
    {
        _rstrL = strSrc.substr(0, iSepPos);
        _rstrR = strSrc.substr(iSepPos + strSep.length(), strSrc.length() - iSepPos - strSep.length());
        return true;
    }
    
}

//-------------------------------------------------------------------------------

const string& module_filename(const HMODULE _hInstance = 0)
{
	static string strModuleFileName;
	if (_hInstance)
	{
		char arrcBuf[MAX_PATH];
		GetModuleFileName(_hInstance, arrcBuf, sizeof(arrcBuf));
		strModuleFileName = arrcBuf;
	}
	return strModuleFileName;
}

//-------------------------------------------------------------------------------

bool ServiceStart(void);
void ServiceRun(void);
void ServiceStop(void);

//-------------------------------------------------------------------------------

SERVICE_STATUS_HANDLE hServiceStatus = 0;
CSatelliteProcess* poApp = 0;
string strCage, strCommandLine;

//-------------------------------------------------------------------------------

BOOL ServiceSignalStatus(const DWORD _dwServiceStatus, const DWORD _dwControlsAccepted, const DWORD _dwWin32ExitCode)
{
    SERVICE_STATUS strucServiceStatus;
    strucServiceStatus.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
    strucServiceStatus.dwCurrentState = _dwServiceStatus;
    strucServiceStatus.dwControlsAccepted = _dwControlsAccepted;
    strucServiceStatus.dwWin32ExitCode = _dwWin32ExitCode;
    strucServiceStatus.dwServiceSpecificExitCode = 0;  
    strucServiceStatus.dwCheckPoint = 0;
    strucServiceStatus.dwWaitHint = 0;
    return SetServiceStatus(hServiceStatus, &strucServiceStatus);
}

//-------------------------------------------------------------------------------

DWORD WINAPI ServiceHandlerEx(DWORD dwControl, DWORD dwEventType, LPVOID lpEventData, LPVOID lpContext)
{
    switch (dwControl)
    {
    case SERVICE_CONTROL_SHUTDOWN: 
    case SERVICE_CONTROL_STOP:
        ServiceStop();
        ServiceSignalStatus(SERVICE_STOPPED, 0, NO_ERROR);
        return NO_ERROR;
    default:
        return ERROR_CALL_NOT_IMPLEMENTED;
    }
}

//-------------------------------------------------------------------------------

VOID WINAPI ServiceEntry(DWORD dwArgc, LPTSTR* lpszArgv)
{

    const string strServiceName = SERVICE_SHORT_NAME_PREFIX + strCage;
    hServiceStatus = RegisterServiceCtrlHandlerEx(strServiceName.c_str(), ServiceHandlerEx, 0); 

    if (hServiceStatus != 0)
    {
        if (ServiceStart())
        {
            ServiceSignalStatus(SERVICE_RUNNING, SERVICE_ACCEPT_SHUTDOWN | SERVICE_ACCEPT_STOP, NO_ERROR);
            ServiceRun();
        }
        else
        {
            ServiceSignalStatus(SERVICE_STOPPED, 0, NO_ERROR);
        }
    }

}

//-------------------------------------------------------------------------------

int EntryInstallService(void)
{

    SC_HANDLE schSCManager = OpenSCManager(0, 0, SC_MANAGER_ALL_ACCESS);
    if (schSCManager == 0) return 1;

    const string strServiceName = SERVICE_SHORT_NAME_PREFIX + strCage;
    const string strFullServiceName = SERVICE_FULL_NAME_PREFIX + strCage;
    const string strServiceDescription = SERVICE_DESCRIPTION_PREFIX + strCage;
    const string strServiceCommandLine = "\"" + module_filename() + "\" run " + strCommandLine;

    SC_HANDLE schService = OpenService(schSCManager, strServiceName.c_str(), SERVICE_QUERY_STATUS);
    if (schService == 0)
    {
                
        schService = CreateService(schSCManager, strServiceName.c_str(), strFullServiceName.c_str(), 
                                   SERVICE_ALL_ACCESS, SERVICE_WIN32_OWN_PROCESS, SERVICE_AUTO_START, 
                                   SERVICE_ERROR_IGNORE, strServiceCommandLine.c_str(), 0, 0, 0, 0, 0);
        if (schService == 0) 
        {
            CloseServiceHandle(schSCManager);
            return 1;
        }

        SERVICE_DESCRIPTION strucServiceDescription = { const_cast<LPSTR>(strServiceDescription.c_str()) };
        ChangeServiceConfig2(schService, SERVICE_CONFIG_DESCRIPTION, &strucServiceDescription);

    }

    CloseServiceHandle(schService);
    CloseServiceHandle(schSCManager);

    MSG((strFullServiceName + " has been installed   ").c_str());
    return 0;

}

//-------------------------------------------------------------------------------

int EntryRemoveService(void)
{

    SC_HANDLE schSCManager = OpenSCManager(0, 0, SC_MANAGER_ALL_ACCESS);
    if (schSCManager == 0) return 1;

	const string strServiceName = SERVICE_SHORT_NAME_PREFIX + strCage;
    const string strFullServiceName = SERVICE_FULL_NAME_PREFIX + strCage;

    SC_HANDLE schService = OpenService(schSCManager, strServiceName.c_str(), DELETE | SERVICE_STOP);
    if (schService != 0)
    {
        SERVICE_STATUS dwUnused; 
        ControlService(schService, SERVICE_CONTROL_STOP, &dwUnused);
        DeleteService(schService);
        CloseServiceHandle(schService);
    }

    CloseServiceHandle(schSCManager);

    MSG(("Service " + strFullServiceName + " has been successfully removed   ").c_str());
    return 0;

}

//-------------------------------------------------------------------------------

int EntryLaunchService(void)
{

	const string strServiceName = SERVICE_SHORT_NAME_PREFIX + strCage;

    SERVICE_TABLE_ENTRY arrstrucServices[] = { { const_cast<LPSTR>(strServiceName.c_str()), 
                                                 ServiceEntry }, { 0, 0 } };
    if (!StartServiceCtrlDispatcher(arrstrucServices))
    {
        return 1;
    }

    return 0;

}

//-------------------------------------------------------------------------------

bool ServiceStart(void)
{
	Sleep(7000);
	poApp = new CSatelliteProcess(strCommandLine, 0);
    return poApp != 0;
}

//-------------------------------------------------------------------------------

void ServiceRun(void)
{
}

//-------------------------------------------------------------------------------

void ServiceStop(void)
{
    delete poApp;
    Sleep(7000);
}

//-------------------------------------------------------------------------------

int __stdcall WinMain(HINSTANCE hInstance, HINSTANCE hPrevInstance, LPTSTR lpCmdLine, int nCmdShow)
{

	module_filename(hInstance);

    // parse command line

    string strCommand;

    strCommandLine = static_cast<const char*>(lpCmdLine);
    split(strCommandLine, " ", strCommand, strCommandLine);
	
    // switch action

    if (strCommand == "install")
    {
	    split(strCommandLine, " ", strCage, strCommandLine);
        return EntryInstallService();
    }
    else if (strCommand == "remove")
    {
	    split(strCommandLine, " ", strCage, strCommandLine);
        return EntryRemoveService();
    }
    else if (strCommand == "run")
    {
        return EntryLaunchService();
    }
    else
    {
        MSG("Pythomnic3k win32 service installer:\n"
            "\n"
            "To install a cage as a service:\n"
            "\n"
			"c:>  pmnc3ksvc.exe  install  cage_name  c:\\python31\\python.exe  \\    \n"
            "        c:\\pythomnic3k\\startup.py  [node_name.]cage_name\n"
            "\n"
            "To remove an installed cage:\n"
            "\n"
			"c:>  pmnc3ksvc.exe  remove  cage_name\n"
			"\n");
        return 1;
    }

}

//-------------------------------------------------------------------------------
