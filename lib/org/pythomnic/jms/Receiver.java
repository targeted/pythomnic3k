/*
# Pythomnic3k Project, http://www.pythomnic.org/
#
# This module implements a command-line application which connects
# to a JMS queue and then keeps receiving messages and delivering
# them to the Python code via stdout.
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
import java.net.*;
import java.util.*;

public class Receiver
{

    /*
    This method parses the name=value parameters passed in the command line
    and puts them all into a property bag.
    */
    private static Properties parseArgs(String[] args) throws Exception
    {

        Properties propResult = new Properties();

        for (int i = 0; i < args.length; ++i)
        {
            String[] strArgSplit = args[i].split("=", 2);
            propResult.setProperty(strArgSplit[0], strArgSplit[1]);
        }

        return propResult;

    }

    /*
    This is the main method. It parses command line arguments,
    then goes into eternal message receiving loop. Each received
    message is sent to the client to be explicitly acknowledged.
    */
    public static void main(String[] args)
    {

        // wrap stdin/stdout into buffered i/o instances

        BufferedWriter stdout = null;
        BufferedReader stdin = null;

        try
        {
            stdout = new BufferedWriter(new OutputStreamWriter(System.out, "ascii"));
            stdin = new BufferedReader(new InputStreamReader(System.in, "ascii"));
        }
        catch (UnsupportedEncodingException _)
        {
            return;
        }

        // set default line delimiters just in case we will need to report a parsing exception

        String strBOL = "4F36095410830A13";
        String strEOL = "92B4782E3B570FD3";

        try
        {

            // parse the command line arguments

            Properties propArgs = parseArgs(args);

            // extract the random delimiters

            strBOL = propArgs.getProperty("stdout.bol");
            strEOL = propArgs.getProperty("stdout.eol");

            // open JMS queue

            QueueConnection connQueue = new QueueConnection(propArgs);
            connQueue.connect();
            try
            {

                // the queue is opened for receiving messages

                connQueue.startConsumer();

                // signal readiness to the client

                Packet pktReady = new Packet();
                pktReady.setProperty("XPmncStatus", "READY");
                pktReady.saveToStream(stdout, 128, strBOL, strEOL);

                // keep receiving messages and sending them to the client
                // having each one explicitly acknowledged

                long lRequestCount = 0;

                while (true)
                {

                    Packet pktRequest = connQueue.receiveMessage();
                    boolean boolNoop = pktRequest.isEmpty();
                    String strRequestID = Long.toString(lRequestCount++);
                    pktRequest.setProperty("XPmncRequestID", strRequestID);

                    if (boolNoop) // no message is available, send a probe to see whether the client is still there
                    {
                        pktRequest.setProperty("XPmncRequest", "NOOP");
                    }
                    else // a message is available
                    {
                        pktRequest.setProperty("XPmncRequest", "RECEIVE");
                    }

                    // send the request to the client and read a response

                    pktRequest.saveToStream(stdout, 128, strBOL, strEOL);
                    Packet pktResponse = Packet.loadFromStream(stdin);

                    if (!pktResponse.getProperty("XPmncRequestID").equals(strRequestID))
                    {
                        throw new Exception("unexpected response");
                    }

                    String strResponse = pktResponse.getProperty("XPmncResponse");

                    if (boolNoop) // got probing response - either OK or EXIT
                    {
                        if (strResponse.equals("OK"))
                        {
                            // do nothing
                        }
                        else if (strResponse.equals("EXIT"))
                        {
                            break;
                        }
                        else
                        {
                            throw new Exception("invalid response");
                        }
                    }
                    else // got message response - either COMMIT, ROLLBACK or EXIT (implies ROLLBACK)
                    {
                        if (strResponse.equals("COMMIT"))
                        {
                            connQueue.commitTransaction();
                        }
                        else if (strResponse.equals("ROLLBACK"))
                        {
                            connQueue.rollbackTransaction();
                        }
                        else if (strResponse.equals("EXIT"))
                        {
                            connQueue.rollbackTransaction();
                            break;
                        }
                        else
                        {
                            throw new Exception("invalid response");
                        }
                    }

                }

            }
            finally
            {
                connQueue.disconnect();
            }

        }
        catch (Exception e)
        {

            // format a single printable line with stack trace

            String strError = e.toString();
            StackTraceElement[] arrStackTrace = e.getStackTrace();
            for (int i = 0; i < arrStackTrace.length; ++i)
            {
                StackTraceElement objStackFrame = arrStackTrace[i];
                strError += " <- " + objStackFrame.getMethodName() + "() in " + 
                            objStackFrame.getFileName() + ":" + objStackFrame.getLineNumber();
            }

            // report the error and exit

            Packet pktError = new Packet();
            pktError.setProperty("XPmncError", strError);
            try
            {
                pktError.saveToStream(stdout, 128, strBOL, strEOL);
            }
            catch (IOException _)
            {
                // do nothing
            }

        }

    }

}