import email
import imaplib
import multiprocessing
import os
import dateutil.parser
from lxml import etree
from sqlalchemy import create_engine
import pandas as pd


def login_email(username, password):
    # 公司qq邮箱
    conn = imaplib.IMAP4_SSL(port='993', host='imap.exmail.qq.com')
    print('已连接服务器')
    conn.login(user=username, password=password)
    print('已登陆')
    return conn

# 163邮箱登录



# 解析发件人和收件人名称和邮件


def decode_title(email_title):
    to_name, to_email = email.utils.parseaddr(email_title)
    to_value, to_charset = email.header.decode_header(to_name)[0]
    if to_charset:
        to_value = to_value.decode(to_charset)
    return to_value, to_email


# 解析内容
def extract_email(msg):
    # 发件人
    from_name, from_email = decode_title(msg.get('From'))
    # 收件人
    to_name, to_email = decode_title(msg.get('To'))
    # 收件时间
    receive_date = str(dateutil.parser.parse(msg.get('Date'), fuzzy=True))
    # 标题，无主题的时候，解析可能有问题，因此msg.get提供一个默认输出
    subdecode, charset = email.header.decode_header(msg.get('Subject', '(无主题)'))[0]
    subject = subdecode.decode(charset) if type(subdecode) == bytes else subdecode
    # message id
    email_id = email.utils.parseaddr(msg.get('message-id'))[1]
    # 解析邮件正文，不包括附件
    cont = []
    for part in msg.walk():
        # each part is a either non-multipart, or another multipart message
        # that contains further parts... Message is organized like a tree
        content_type = part.get_content_type()
        content_charset = part.get_content_charset()
        if content_type == 'text/plain':
            content = part.get_payload(decode=True).decode(
                str(content_charset), 'ignore')
            cont.append(content)
        elif content_type == 'text/html':
            content = part.get_payload(decode=True).decode(
                str(content_charset), 'ignore')
            s = etree.HTML(content)
            content = ''.join(s.xpath('//text()'))
            cont.append(content)
    content = '^^^^^^^^'.join(cont)
    email_content = {
        'email_id': email_id,
        'subject': subject,
        'from_email': from_email,
        'to_email': to_email,
        'receive_date': receive_date,
        'content': content
    }
    # print(email_content)
    return email_content


# 下载附件
def load_attachment(emailbody):
    mail = email.message_from_bytes(emailbody)

    # 下载附件
    fileName = '没有找到任何附件！'
    # 获取邮件附件名称
    for part in mail.walk():
        if part.get_content_maintype() == 'multipart':
            continue
        if part.get('Content-Disposition') is None:
            continue
        fileName = part.get_filename()

        # 如果文件名为纯数字、字母时不需要解码，否则需要解码
        try:
            fileName = email.header.decode_header(fileName)[0][0].decode(email.header.decode_header(fileName)[0][1])
        except Exception as e:
            print(fileName, e)
            pass
        # 如果获取到了文件，则将文件保存在制定的目录下
        if fileName != '没有找到任何附件！':
            current_path = os.getcwd()
            filePath = os.path.join(current_path, fileName)

            if not os.path.isfile(filePath):
                fp = open(filePath, 'wb')
                fp.write(part.get_payload(decode=True))
                fp.close()
                print("附件已经下载，文件名为：" + fileName)
            else:
                print("附件已经存在，文件名为：" + fileName)


# params = (username, password, 4)


def load_email(params):
    """
    username：邮箱
    password：密码
    idx：邮件的索引，从1开始
    """
    username, password, idx = params
    # 收件箱列表
    conn = login_email(username, password)

    # 选择收件箱-用户意见反馈收件箱
    box_list = conn.list()[1]
    conn.select('INBOX')
    type1, email_indexes = conn.search(None, 'ALL')

    # 如果我们要取回最新一封邮件可以把newlist[0]传递给fetch()
    newlist = email_indexes[0].split()
    try:
        type2, email_data = conn.fetch(newlist[-idx], '(RFC822)')
        emailbody = email_data[0][1]
        # 获取邮件内容
        email_string = email_data[0][1].decode('utf-8') if type(emailbody) == bytes else emailbody
        msg = email.message_from_string(email_string)
        email_content = extract_email(msg)
        # 下载邮件附件
        load_attachment(emailbody)
        return email_content
    except Exception as e:
        print(e)
        return {}


# 连接mysql并存入mysql


class Mysql(object):
    def __init__(self, mysql_user, mysql_password, db):
        self.__user = mysql_user
        self.__password = mysql_password
        self.engine = create_engine(
            f'mysql+pymysql://{mysql_user}:{mysql_password}@127.0.0.1:3306/{db}'
        )

    def execute_sql(self, sql):
        res = self.engine.execute(sql)
        return res


# 连接mysql，如果最新一条邮件是用户反馈，且不在当前表中，则插入mysql
mysql_user = 'root'
mysql_password = '9870384@TapTap'
db = 'tap'
engine = create_engine(f'mysql+pymysql://{mysql_user}:{mysql_password}@127.0.0.1:3306/{db}')

# 获取邮件
username = 'houdongdong@taptap.com'
password = 'FvJY6gdcMS56VF7D'

# 获取最新一条邮件
email_content = load_email((username, password, 1))

# 读取附件文件为pandas dataframe
dfs = pd.read_excel('strategy_push_users.xlsx', sheet_name=None)
for tb in dfs.keys():
    print(tb)
    df = dfs.get('strategy_push_users')
    df.to_sql(name=tb, con=engine, if_exists='append', index=False)
    print('写入mysql完成！')
