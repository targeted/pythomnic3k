/*
# Pythomnic3k Project, http://www.pythomnic.org/
#
# This module implements a command-line application which connects
# to a JMS queue and then keeps receiving messages from Python code
# via stdin and send them to the JMS queue.
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

public class Sender
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
    then goes into eternal message sending loop. Each message
    should be sent in a separate transaction.
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

                // the queue is opened for sending messages

                connQueue.startProducer();

                // signal readiness to the client

                Packet pktReady = new Packet();
                pktReady.setProperty("XPmncStatus", "READY");
                pktReady.saveToStream(stdout, 128, strBOL, strEOL);

                // keep reading and processing commands arriving
                // from stdin and writing the results to stdout

                while (true)
                {

                    Packet pktRequest = Packet.loadFromStream(stdin);
                    String strRequest = pktRequest.getProperty("XPmncRequest");
                    String strRequestID = pktRequest.getProperty("XPmncRequestID");

                    Packet pktResponse = new Packet();
                    pktResponse.setProperty("XPmncRequestID", strRequestID);

                    if (strRequest.equals("SEND"))
                    {
                        String strMessageID = connQueue.sendMessage(pktRequest);
                        pktResponse.setProperty("XPmncMessageID", strMessageID);
                    }
                    else if (strRequest.equals("COMMIT"))
                    {
                        connQueue.commitTransaction();
                    }
                    else if (strRequest.equals("ROLLBACK"))
                    {
                        connQueue.rollbackTransaction();
                    }
                    else if (strRequest.equals("NOOP") || strRequest.equals("EXIT"))
                    {
                        // do nothing
                    }
                    else
                    {
                        throw new Exception("invalid request");
                    }

                    pktResponse.setProperty("XPmncResponse", "OK"); // always a success unless an exception
                    pktResponse.saveToStream(stdout, 128, strBOL, strEOL);

                    if (strRequest.equals("EXIT"))
                    {
                        break;
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