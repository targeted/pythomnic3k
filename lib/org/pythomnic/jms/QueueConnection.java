/*
# Pythomnic3k Project, http://www.pythomnic.org/
#
# This module implements a class with a small set of utility methods
# to connect to a JMS queue and send/receive a message.
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

import java.io.*;
import java.util.*;
import javax.jms.*;
import javax.naming.*;

public class QueueConnection
{

    private Properties m_propJNDIArgs;
    private Properties m_propConnectionArgs;

    private InitialContext m_objJNDIContext = null;
    private Connection m_objConnection = null;
    private Destination m_objQueue = null;
    private Session m_objSession = null;
    private MessageProducer m_objProducer;
    private MessageConsumer m_objConsumer;
    private boolean m_boolStarted = false;

    public QueueConnection(Properties _propArgs)
    {
        m_propJNDIArgs = filterProperties(_propArgs, "jndi.");
        m_propConnectionArgs = filterProperties(_propArgs, "connection.");
    }

    /*
    Extracts properties whose keys start with the specified prefix into a separate property bag.
    */
    private static Properties filterProperties(Properties _propValues, String _strPrefix)
    {

        Properties propResult = new Properties();

        Iterator iterEntries = _propValues.entrySet().iterator();
        while (iterEntries.hasNext())
        {

            Map.Entry entKeyValue = (Map.Entry)iterEntries.next();
            String strKey = (String)entKeyValue.getKey();
            String strValue = (String)entKeyValue.getValue();

            if (strKey.startsWith(_strPrefix))
            {
                propResult.setProperty(strKey.substring(_strPrefix.length()), strValue);
            }

        }

        return propResult;

    }

    /*
    Perform full connection to a JMS queue except for producer/consumer selection.
    */
    public void connect() throws JMSException, NamingException
    {

        // prepare JNDI context

        m_objJNDIContext = new InitialContext(m_propJNDIArgs);

        // load connection factory by name

        String strConnectionFactoryName = m_propConnectionArgs.getProperty("factory");
        if (strConnectionFactoryName == null)
        {
            throw new IllegalArgumentException("connection factory name is not specified");
        }

        ConnectionFactory objConnectionFactory =
            (ConnectionFactory)m_objJNDIContext.lookup(strConnectionFactoryName);

        // open JMS connection

        String strUsername = m_propConnectionArgs.getProperty("username");
        String strPassword = m_propConnectionArgs.getProperty("password");

        if (strUsername == null || strPassword == null) // use anonymous authentication
        {
            m_objConnection = objConnectionFactory.createConnection();
        }
        else // use username/password authentication
        {
            m_objConnection = objConnectionFactory.createConnection(strUsername, strPassword);
        }

        // lookup the specified queue

        String strQueueName = m_propConnectionArgs.getProperty("queue");
        if (strQueueName == null)
        {
            throw new IllegalArgumentException("queue name is not specified");
        }

        m_objQueue = (Destination)m_objJNDIContext.lookup(strQueueName);

        // create a transacted session with manual acknowledgement

        m_objSession = m_objConnection.createSession(true, Session.CLIENT_ACKNOWLEDGE);

        if (!m_objSession.getTransacted())
        {
            m_objSession.close();
            m_objSession = null;
            throw new IllegalArgumentException("the created session is not transacted");
        }

    }

    /*
    Become a producer.
    */
    public void startProducer() throws JMSException
    {
        m_objProducer = m_objSession.createProducer(m_objQueue);
        m_objConnection.start();
        m_boolStarted = true;
    }

    /*
    Become a consumer.
    */
    public void startConsumer() throws JMSException
    {
        m_objConsumer = m_objSession.createConsumer(m_objQueue);
        m_objConnection.start();
        m_boolStarted = true;
    }

    /*
    Send a text message represented with the named packet fields.
    */
    public String sendMessage(Packet _pktRequest) throws JMSException
    {

        // create a text message

        String strMessageText = _pktRequest.getProperty("XPmncMessageText");
        _pktRequest.remove("XPmncMessageText");
        Message msgMessage = m_objSession.createTextMessage(strMessageText);

        // extract JMS-specific parameters and named header fields
        // and attach them to the created message

        Iterator iterHeaders = _pktRequest.entrySet().iterator();
        while (iterHeaders.hasNext())
        {

            Map.Entry entKeyValue = (Map.Entry)iterHeaders.next();
            String strKey = (String)entKeyValue.getKey();
            String strValue = (String)entKeyValue.getValue();

            if (strKey.equals("JMSCorrelationID"))
            {
                msgMessage.setJMSCorrelationID(strValue);
            }
            else if (strKey.equals("JMSDeliveryMode"))
            {
                msgMessage.setJMSDeliveryMode(new Integer(strValue).intValue());
            }
            else if (strKey.equals("JMSExpiration"))
            {
                msgMessage.setJMSExpiration(new Long(strValue).longValue());
            }
            else if (strKey.equals("JMSPriority"))
            {
                msgMessage.setJMSPriority(new Integer(strValue).intValue());
            }
            else if (strKey.equals("JMSRedelivered"))
            {
                msgMessage.setJMSRedelivered(new Boolean(strValue).booleanValue());
            }
            else if (strKey.equals("JMSTimestamp"))
            {
                msgMessage.setJMSTimestamp(new Long(strValue).longValue());
            }
            else if (strKey.equals("JMSType"))
            {
                msgMessage.setJMSType(strValue);
            }
            else if (!strKey.startsWith("XPmnc")) // the default behaviour is to attach a named string property
            {
                msgMessage.setStringProperty(strKey, strValue);
            }

        }

        // actually send the message

        m_objProducer.send(msgMessage);

        // after the message has been sent, message id is set by the provider

        return msgMessage.getJMSMessageID();

    }

    /*
    Utility, null -> ""
    */
    private String nullToEmptyString(String _strValue)
    {
        return _strValue == null ? "" : _strValue;
    }

    /*
    Wait to receive a message, return the message content parsed into
    named packet fields or empty packet if there is no message.
    */
    public Packet receiveMessage() throws JMSException
    {

        Packet pktResult = new Packet();

        Message msgRawMessage = m_objConsumer.receive(3000); // wait for at most 3 seconds
        if (msgRawMessage == null)
        {
            return pktResult;
        }

        if (!(msgRawMessage instanceof TextMessage))
        {
            throw new JMSException("unsupported message type");
        }

        TextMessage msgMessage = (TextMessage)msgRawMessage;

        // extract message text

        pktResult.setProperty("XPmncMessageText", msgMessage.getText());

        // extract JMS-specific parameters

        pktResult.setProperty("JMSCorrelationID", nullToEmptyString(msgMessage.getJMSCorrelationID()));
        pktResult.setProperty("JMSDeliveryMode", new Integer(msgMessage.getJMSDeliveryMode()).toString());
        pktResult.setProperty("JMSExpiration", new Long(msgMessage.getJMSExpiration()).toString());
        pktResult.setProperty("JMSMessageID", nullToEmptyString(msgMessage.getJMSMessageID()));
        pktResult.setProperty("JMSPriority", new Integer(msgMessage.getJMSPriority()).toString());
        pktResult.setProperty("JMSRedelivered", new Boolean(msgMessage.getJMSRedelivered()).toString());
        pktResult.setProperty("JMSTimestamp", new Long(msgMessage.getJMSTimestamp()).toString());
        pktResult.setProperty("JMSType", nullToEmptyString(msgMessage.getJMSType()));

        // extract just the name of the queue where the reply message is expected

        Destination objReplyTo = msgMessage.getJMSReplyTo();
        String strReplyTo;

        if (objReplyTo == null)
        {
            strReplyTo = "";
        }
        else if (objReplyTo instanceof javax.jms.Queue)
        {
            strReplyTo = nullToEmptyString(((javax.jms.Queue)objReplyTo).getQueueName());
        }
        else if (objReplyTo instanceof javax.jms.Topic)
        {
            strReplyTo = nullToEmptyString(((javax.jms.Topic)objReplyTo).getTopicName());
        }
        else
        {
            strReplyTo = "";
        }

        pktResult.setProperty("JMSReplyTo", strReplyTo);

        // extract additional parameters

        Enumeration enumPropertyNames = msgMessage.getPropertyNames();
        while (enumPropertyNames.hasMoreElements())
        {
            String strPropertyName = (String)enumPropertyNames.nextElement();
            pktResult.setProperty(strPropertyName, msgMessage.getStringProperty(strPropertyName));
        }

        return pktResult;

    }

    /*
    Commit a session transaction, no matter sending or receiving.
    */
    public void commitTransaction() throws JMSException
    {
        m_objSession.commit();
    }

    /*
    Rollback a session transaction, no matter sending or receiving.
    */
    public void rollbackTransaction() throws JMSException
    {
        m_objSession.rollback();
    }

    /*
    Perform full disconnection from a JMS queue.
    */
    public void disconnect() throws JMSException, NamingException
    {

        try
        {
            if (m_boolStarted) m_objConnection.stop();
            if (m_objConsumer != null) m_objConsumer.close();
            if (m_objProducer != null) m_objProducer.close();
            if (m_objSession != null) m_objSession.close();
        }
        finally
        {
            if (m_objConnection != null) m_objConnection.close();
        }

        if (m_objJNDIContext != null) m_objJNDIContext.close();

    }

}
