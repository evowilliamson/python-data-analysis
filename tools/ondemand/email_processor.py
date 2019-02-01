from misc.singleton import Singleton
import utils
import datetime
from config import Config, EXPIRE_PERIOD

TIME_FORMAT = "%Y-%m-%d"


@Singleton
class EmailProcessor:
    """
    Class responsable for the generation of the emails. One email per recipient should be generated. Each
    email will contains all the requests that will expire
    """

    def __init__(self, requests):
        """ Default constructor """

        self._requests = requests

    def process(self):
        """
        Processes all the requests and groups the requests per owner (email). Then, the mails are sent to the recipients
        :return:
        """

        self.generate_emails(self.create_recipient_requests_mapping())

    def generate_emails(self, recipients_requests):
        """
        This method generates the emails. Per recipient, one email is sent with all the requests
        :param recipients_requests: the recipient-to-request mapping
        :return:
        """
        # sort the request for every recipient according to the end_time, ascending, first in the
        # list, first to expire
        for recipient, requests in recipients_requests.items():
            requests.sort(key=lambda request: request.end_time)
            utils.send_mail(
                subject="On-demand ADC request expirations",
                body=self.make_email_body(requests),
                recipient=recipient)

    def create_recipient_requests_mapping(self):
        """
        Method that creates the mapping of a recipient to his/her requests. A recipient should only
        receive one email with the request that will expire. In order to achieve this, a mapping needs to be
        created that groups all requests affected per recipient
        :return: the mapping
        """
        recipients_requests = {}
        for __request in self._requests:
            if (__request.end_time - datetime.datetime.now()).days < int(Config.instance().config[EXPIRE_PERIOD]) and \
                    __request.end_time > datetime.datetime.now():
                # Only send mail if there is less than 50 days left for ther request to expire
                print("The following request will expire in less than 50 days and will be reported: {0}".
                      format(str(__request)))
                emails = __request.emails.split(",")
                for email in emails:
                    if email not in recipients_requests:
                        recipients_requests[email] = [__request]
                    else:
                        recipients_requests[email].append(__request)
        return recipients_requests

    @staticmethod
    def make_email_body(requests):
        """
        Method that creates the body of an email
        :param requests: the requests that serve as an input to the email, i.e. the contents
        :return:
        """

        mail_txt = EmailProcessor.create_style_and_table_header_html()
        mail_txt += EmailProcessor.create_requests_table_html(requests)
        mail_txt += EmailProcessor.create_footer_html()
        return mail_txt

    @staticmethod
    def create_footer_html():
        """
        This method creates the footer html of the email
        :return: the footer table html
        """
        return """
                </table>
                <br><br>This mail was automatically generated and will be send out every week. For inquiries, 
                please send a mail to: dl-new@test.com
            </body>"""

    @staticmethod
    def create_requests_table_html(requests):
        """
        This method creates the html that contains the information regarding the requests that will expire
        :param requests: the list of request to be reported to expire
        :return: string that contains the html with the requests
        """
        mail_txt = ""
        for i, request in enumerate(requests):
            mail_txt += ("""<tr>
                                <td class="{6}">{0}</td>
                                <td class="{6}">{1}</td>
                                <td class="{6}">{2}</td>
                                <td class="{6}">{3}</td>
                                <td class="{6}">{4}</td>
                                <td class="{6}">{5}</td>
                            </tr>""".format(
                request.id, request.description, request.component,
                request.start_time.strftime(TIME_FORMAT),
                request.end_time.strftime(TIME_FORMAT), request.owner,
                "row_column_odd" if i % 2 == 0 else "row_column_even"))
        return mail_txt

    @staticmethod
    def create_style_and_table_header_html():
        """
        Method that generates the html for the meta part and the table definition
        :return: the html with meta information and the table definition
        """
        return """<head>
          <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
            <style type="text/css">
            .tg  {border-collapse:collapse;border-spacing:0;border-style:none;}
            .tg .row_column_even{background-color:#ddddb1;border-style:none;vertical-align:top;text-align:left;padding: 10px;}
            .tg .row_column_odd{background-color:#f5f1ea;border-style:none;vertical-align:top;text-align:left;padding: 10px}
            .tg .header_column{background-color:#c0c0c0;border-style:none;vertical-align:top;text-align:left;padding: 10px}
            </style>
          </meta>
        </head>
        <body>The following on-demand ADC requests where detected to expire in the near future:<br><br>
            <table class="tg">
            <colgroup>
            </colgroup>
              <tr>
                <th class="header_column" width="10%">Request id</th>
                <th class="header_column" width="30%">Description</th>
                <th class="header_column" width="10%">Component</th>
                <th class="header_column" width="20%">Start date<br></th>
                <th class="header_column" width="20%">Expiration date<br></th>
                <th class="header_column" width="10%">Owner</th>
              </tr>
            """
