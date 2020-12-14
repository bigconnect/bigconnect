/*
 * This file is part of the BigConnect project.
 *
 * Copyright (c) 2013-2020 MWARE SOLUTIONS SRL
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License version 3
 * as published by the Free Software Foundation with the addition of the
 * following permission added to Section 15 as permitted in Section 7(a):
 * FOR ANY PART OF THE COVERED WORK IN WHICH THE COPYRIGHT IS OWNED BY
 * MWARE SOLUTIONS SRL, MWARE SOLUTIONS SRL DISCLAIMS THE WARRANTY OF
 * NON INFRINGEMENT OF THIRD PARTY RIGHTS

 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program; if not, see http://www.gnu.org/licenses or write to
 * the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA, 02110-1301 USA, or download the license from the following URL:
 * https://www.gnu.org/licenses/agpl-3.0.txt
 *
 * The interactive user interfaces in modified source and object code versions
 * of this program must display Appropriate Legal Notices, as required under
 * Section 5 of the GNU Affero General Public License.
 *
 * You can be released from the requirements of the license by purchasing
 * a commercial license. Buying such a license is mandatory as soon as you
 * develop commercial activities involving the BigConnect software without
 * disclosing the source code of your own applications.
 *
 * These activities include: offering paid services to customers as an ASP,
 * embedding the product in a web application, shipping BigConnect with a
 * closed source product.
 */
package com.mware.core.email;

import com.google.common.base.Joiner;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.mware.core.config.Configuration;
import com.mware.core.exception.BcException;
import com.mware.core.util.BcLogger;
import com.mware.core.util.BcLoggerFactory;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.StringWriter;
import java.util.Date;
import java.util.Properties;

@Singleton
public class SmtpEmailRepository implements EmailRepository {
    private static final BcLogger LOGGER = BcLoggerFactory.getLogger(SmtpEmailRepository.class);
    private static final String CHARSET = "UTF-8";
    private SmtpEmailConfiguration smtpEmailConfiguration;

    @Inject
    public SmtpEmailRepository(Configuration configuration) {
        smtpEmailConfiguration = new SmtpEmailConfiguration();
        configuration.setConfigurables(smtpEmailConfiguration, SmtpEmailConfiguration.CONFIGURATION_PREFIX);
    }

    @Override
    public void send(String fromAddress, String toAddress, String subject, String body) {
        send(fromAddress, new String[]{toAddress}, subject, body);
    }

    @Override
    public void send(String fromAddress, String[] toAddresses, String subject, String body) {
        String joinedToAddresses = Joiner.on(",").join(toAddresses);
        LOGGER.debug("sending SMTP email from: \"%s\", to: \"%s\", subject: \"%s\"", fromAddress, joinedToAddresses, subject);
        LOGGER.debug("sending SMTP email body:%n%s", body);

        try {
            MimeMessage mimeMessage = new MimeMessage(getSession());
            mimeMessage.setFrom(InternetAddress.parse(fromAddress)[0]);
            mimeMessage.setSubject(subject, CHARSET);
            if (body.startsWith("<html>")) {
                Multipart multipart = new MimeMultipart();
                MimeBodyPart html = new MimeBodyPart();
                String contentType = "text/html; charset=" + CHARSET;
                html.setHeader("Content-Type", contentType);
                html.setContent(body, contentType);
                multipart.addBodyPart(html);
                mimeMessage.setContent(multipart);
            } else {
                mimeMessage.setText(body, CHARSET);
            }
            mimeMessage.setSentDate(new Date());
            mimeMessage.setRecipients(Message.RecipientType.TO, InternetAddress.parse(joinedToAddresses));
            Transport.send(mimeMessage);
        } catch (MessagingException me) {
            throw new BcException("exception while sending email", me);
        }
    }

    @Override
    public Session getSession() {
        Properties properties = new Properties();
        properties.put("mail.smtp.host", smtpEmailConfiguration.getServerHostname());
        properties.put("mail.smtp.port", smtpEmailConfiguration.getServerPort());
        Authenticator authenticator = null;

        switch (smtpEmailConfiguration.getServerAuthentication()) {
            case NONE:
                // no additional properties required
                break;
            case TLS:
                properties.put("mail.smtp.auth", "true");
                properties.put("mail.smtp.starttls.enable", "true");
                authenticator = getAuthenticator();
                break;
            case SSL:
                properties.put("mail.smtp.auth", "true");
                properties.put("mail.smtp.socketFactory.port", smtpEmailConfiguration.getServerPort());
                properties.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
                authenticator = getAuthenticator();
                break;
            default:
                throw new BcException("unexpected MailServerAuthentication: " + smtpEmailConfiguration.getServerAuthentication().toString());
        }

        Session session = Session.getDefaultInstance(properties, authenticator);
        if (LOGGER.isTraceEnabled()) {
            session.setDebugOut(new LoggerPrintStream(LOGGER));
            session.setDebug(true);
        }
        return session;
    }

    private Authenticator getAuthenticator() {
        return new Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(smtpEmailConfiguration.getServerUsername(), smtpEmailConfiguration.getServerPassword());
            }
        };
    }

    private class LoggerPrintStream extends PrintStream {
        public LoggerPrintStream(final BcLogger logger) {
            super(new OutputStream() {
                private final int NEWLINE = "\n".getBytes()[0];
                private final StringWriter buffer = new StringWriter();

                @Override
                public void write(int c) throws IOException {
                    if (c == NEWLINE) {
                        logger.trace(buffer.toString());
                        buffer.getBuffer().setLength(0);
                    } else {
                        buffer.write(c);
                    }
                }
            });
        }
    }
}
