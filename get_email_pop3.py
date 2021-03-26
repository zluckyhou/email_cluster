# coding:utf-8

# 通过POP3下载邮件
import poplib
from email.parser import Parser
from email.header import decode_header
from email.utils import parseaddr
import json
import multiprocessing
import time


# 邮件的Subject或者Email中包含的名字都是经过编码后的str，要正常显示，就必须decode：
def decode_str(s):
    value, charset = decode_header(s)[0]
    if charset:
        value = value.decode(charset)
    return value


# 文本邮件的内容也是str，还需要检测编码，否则，非UTF-8编码的邮件都无法正常显示：
def guess_charset(msg):
    charset = msg.get_charset()
    if charset is None:
        content_type = msg.get('Content-Type', '').lower()
        pos = content_type.find('charset=')
        if pos >= 0:
            charset = content_type[pos + 8:].strip()
    return charset


# 构造函数用于解析message对象
# Message对象本身可能是一个MIMEMultipart对象，即包含嵌套的其他MIMEBase对象，嵌套可能还不止一层。
# 所以我们要递归地打印出Message对象的层次结构：


def print_info(msg, indent=0, cont=[], header_d={}):
    if indent == 0:
        # 将header存为dict
        for header in ['From', 'To', 'Subject']:
            value = msg.get(header, '')
            if value:
                if header == 'Subject':
                    value = decode_str(value)
                else:
                    hdr, addr = parseaddr(value)
                    name = decode_str(hdr)
                    value = u'%s <%s>' % (name, addr)
            header_d[header] = value
            print('%s%s: %s' % ('  ' * indent, header, value))
    if (msg.is_multipart()):
        parts = msg.get_payload()
        for n, part in enumerate(parts):
            print('%spart %s' % ('  ' * indent, n))
            print('%s--------------------' % ('  ' * indent))
            print_info(part, indent + 1)
    else:
        content_type = msg.get_content_type()
        if content_type == 'text/plain' or content_type == 'text/html':
            content = msg.get_payload(decode=True)
            charset = guess_charset(msg)
            if charset:
                content = content.decode(charset)
                cont.append(content)
            print('%sText: %s' % ('  ' * indent, content + '...'))

        else:
            print('%sAttachment: %s' % ('  ' * indent, content_type))
    return {'header': header_d, 'content': cont}


# 登录邮箱
def login_email(email, password):
    pop3_server = 'pop.exmail.qq.com'
    # 连接到POP3服务器:
    server = poplib.POP3(pop3_server)
    # 可以打开或关闭调试信息:
    # server.set_debuglevel(1)
    # 可选:打印POP3服务器的欢迎文字:
    # print(server.getwelcome().decode('utf-8'))

    # 身份认证:
    server.user(email)
    server.pass_(password)
    # stat()返回邮件数量和占用空间:
    # print('Messages: %s. Size: %s' % server.stat())
    # list()返回所有邮件的编号:
    resp, mails, octets = server.list()

    # 可以查看返回的列表类似[b'1 82923', b'2 2184', ...]
    # print(mails)
    return server, mails


# 打印一封邮件
def get_latest_email(params):
    '''
    idx：要获取的邮件索引，从1开始，1表示最新一封
    '''
    email, password, idx = params
    server, mails = login_email(email=email, password=password)
    # 获取最新一封邮件, 注意索引号从1开始:
    length = len(mails)
    index = range(1, length + 1)
    resp, lines, octets = server.retr(index[-idx])

    # lines存储了邮件的原始文本的每一行,
    # 可以获得整个邮件的原始文本:
    try:
        msg_content = b'\r\n'.join(lines).decode('utf-8')
        # 稍后解析出邮件:msg是一个message对象，
        msg = Parser().parsestr(msg_content)
        email = print_info(msg)
        # 关闭连接:
        server.quit()
    except Exception as e:
        email = {}
    return email


if __name__ == '__main__':
    # 获取历史邮件数据
    t1 = time.time()
    params_ls = [('houdongdong@taptap.com', 'FvJY6gdcMS56VF7D', idx) for idx in range(1, 1783)]
    cpu_cnt = multiprocessing.cpu_count()
    with multiprocessing.Pool(cpu_cnt) as p:
        data = p.map(get_latest_email, params_ls)
    with open('his_email.json', 'w') as f:
        json.dump(data, f)
    t2 = time.time()
    print(f'耗时: {t2 - t1}秒')
