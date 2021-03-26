import email
import imaplib
import multiprocessing
import re

import dateutil.parser
from lxml import etree
from sqlalchemy import create_engine


def login_email(username, password):
    conn = imaplib.IMAP4_SSL(port='993', host='imap.exmail.qq.com')
    print('已连接服务器')
    conn.login(user=username, password=password)
    print('已登陆')
    return conn


# 解析发件人和收件人名称和邮件


def decode_title(email_title):
    # 如果收件人有多个，需要对每个收件人进行处理
    email_title_ls = email_title.split(',')
    name_ls, email_ls = [], []
    for addr in email_title_ls:
        mail_name, mail_addr = email.utils.parseaddr(addr)
        mail_name, to_charset = email.header.decode_header(mail_name)[0]
        if to_charset:
            mail_name = mail_name.decode(to_charset)
        name_ls.append(mail_name)
        email_ls.append(mail_addr)
    return ','.join(name_ls), ','.join(email_ls)


def extract_email(msg):
    # 发件人
    from_name, from_email = decode_title(msg.get('From'))
    # 收件人
    to_name, to_email = decode_title(msg.get('To'))
    # 收件时间
    receive_date = str(dateutil.parser.parse(msg.get('Date'), fuzzy=True))
    # 标题，无主题的时候，解析可能有问题，因此msg.get提供一个默认输出
    subdecode, charset = email.header.decode_header(msg.get(
        'Subject', '(无主题)'))[0]
    subject = subdecode.decode(charset) if type(
        subdecode) == bytes else subdecode
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
    # box_list = conn.list()[1]
    conn.select('&UXZO1mWHTvZZOQ-/&dShiN2EPicFTzZmI-')
    type1, email_indexes = conn.search(None, 'ALL')

    # 如果我们要取回最新一封邮件可以把newlist[0]传递给fetch()
    newlist = email_indexes[0].split()
    try:
        type2, email_data = conn.fetch(newlist[-idx], '(RFC822)')
        email_string = email_data[0][1].decode('utf-8') if type(
            email_data[0][1]) == bytes else email_data[0][1]
        msg = email.message_from_string(email_string)
        email_content = extract_email(msg)
        return email_content
    except Exception as e:
        print(e)
        return {}


def load_his_emails(username, password):
    """
    username：邮箱
    password：密码
    idx：邮件的索引，从1开始
    """
    params_ls = [(username, password, idx) for idx in range(1, 1710)]
    cpu_cnt = multiprocessing.cpu_count()
    with multiprocessing.Pool(cpu_cnt) as p:
        email_ls = p.map(load_email, params_ls)
    return email_ls


# 历史邮件
# t1 = time.time()
# email_ls = load_his_emails(username=username, password=password)
# with open('email_his_ls.json', 'w') as f:
#     json.dump(email_ls, f)
# t2 = time.time()
# print(f'耗时: {t2-t1}秒')

# 历史数据存入数据库
# mysql_user = 'root'
# mysql_password = '9870384@TapTap'
# db = 'tap'
# engine = create_engine(
#     f'mysql+pymysql://{mysql_user}:{mysql_password}@127.0.0.1:3306/{db}')
# with open('email_his_ls.json') as f:
#     email_ls = json.load(f)
# emails = [i for i in email_ls if i]
# df = pd.DataFrame(emails)
# df['content'] = df['content'].apply(lambda x: '^^^^^^^^'.join(x))
# df_advise = df[df['to_email'] == 'webmaster@taptap.com']
# df_advise.to_sql('user_advise_email',
#                  con=engine,
#                  index=False,
#                  if_exists='replace')

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


def insert_email(idx):
    username = 'houdongdong@taptap.com'
    password = 'FvJY6gdcMS56VF7D'

    # 获取最新一条邮件
    email_content = load_email((username, password, idx))

    # 连接mysql，如果最新一条邮件是用户反馈，且不在当前表中，则插入mysql
    mysql_user = 'root'
    mysql_password = '9870384@TapTap'
    db = 'tap'

    mysql = Mysql(mysql_user=mysql_user, mysql_password=mysql_password, db=db)
    if 'webmaster@taptap.com' in email_content.get('to_email'):
        email_id = email_content.get("email_id")
        receive_date = email_content.get("receive_date")
        check_exists_sql = f'select * from user_advise_email where email_id="{email_id}" and receive_date="{receive_date}"'
        res = mysql.execute_sql(check_exists_sql).fetchall()
        if not res:
            email_id = email_content.get("email_id")
            subject = email_content.get("subject")
            from_email = email_content.get("from_email")
            to_email = email_content.get("to_email")
            receive_date = email_content.get("receive_date")
            content = email_content.get("content")
            content = re.sub('["&;]', '', content)
            insert_sql = f'insert into user_advise_email (email_id,subject,from_email,to_email,receive_date,content) values ("{email_id}","{subject}","{from_email}","{to_email}","{receive_date}","{content}")'
            mysql.execute_sql(insert_sql)
            print(f'insert complete! email_id: {email_id}')
            return True
        else:
            return False
    else:
        return True


idx = 1489
flag = True
while flag:
    print(f'正在获取第{idx}封邮件')
    try:
        flag = insert_email(idx=idx)
        idx += 1
    except Exception as e:
        print(idx, e)
        flag = True
        idx += 1
