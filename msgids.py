# coding=utf-8 

import ctypes
import cPickle
import struct

c2i_init_job = 0
i2c_no_resource = 1

# msglen is msghdr + msgdata
class serilizer(object):
    def __init__(self):
        self.send_buffer_max_size = 1024*1024*1024
        self.send_buf = ctypes.create_string_buffer(self.send_buffer_max_size)  
        self.recv_buffer = []
        self.recv_buffer_size = 0
        
    def encode(self,msgid, msgdata):
        str = cPickle.dumps(msgdata)
        msgdatalen = len(str)
        values = (msgid, msgdatalen + 3, str)
        if self.send_buffer_max_size < msgdatalen + 3:
            struct.pack("!BH%ds" % msgdatalen, *values)
        else:
            struct.pack_into("!BH%ds" % msgdatalen, self.send_buf, 0, *values)
        #print("len {%d}\n" % (msgdatalen + 3))
        return self.send_buf[:msgdatalen + 3]
        
    def decode(self,data):
        ret = []
        recvlen = len(data)
        totallen = recvlen + self.recv_buffer_size
        while totallen > 0:
            if totallen < 3 :
                self.recv_buffer.append(data)
                self.recv_buffer_size = totallen
                break
            
            if len(self.recv_buffer) > 0:
                # print("40 sz.decode()::len(recv_buffer) > 0")
                self.recv_buffer.append(data)
                buf = "".join(self.recv_buffer)
                self.recv_buffer = []
                self.recv_buffer_size = 0
            else:
                # print("46 sz.decode()::len(recv_buffer) == 0")
                buf = data
            
            msghdr = struct.unpack_from("!BH", buf, 0)
            msglen = msghdr[1]
        
            if totallen < msglen:
                self.recv_buffer.append(buf)
                self.recv_buffer_size = totallen
                break 
            
            if len(self.recv_buffer) > 0:
                # print("58 sz.decode()::len(recv_buffer) > 0")
                self.recv_buffer.append(buf)
                buf = "".join(recv_buffer)
                self.recv_buffer = []
                self.recv_buffer_size = 0
        #         else:
        #             print("64 sz.decode()::len(recv_buffer) == 0")
                
            # print (len(buf), msglen)
            ret.append((msghdr[0], cPickle.loads(struct.unpack_from("!%ds" % (msglen - 3), buf, 3)[0])))
            totallen -= msglen
            data = buf[msglen:]
        return ret
 
if __name__ == '__main__':
    sz = serilizer()
    print("test sz.decode a complete msg")
    bytesdata = sz.encode(c2i_init_job, "jakez")
    msgs = sz.decode(bytesdata)
    for msg in msgs:
        print("msgid %s,\n msgdata %s\n" % msg)
    
    print("test sz.decode an uncomplete msg")
    sz.decode(bytesdata[0:1])
    sz.decode(bytesdata[1:2])
    sz.decode(bytesdata[2:3])
    sz.decode(bytesdata[3:4])
    sz.decode(bytesdata[4:5])
    sz.decode(bytesdata[5:6])
    sz.decode(bytesdata[6:7])
    sz.decode(bytesdata[7:8])
    sz.decode(bytesdata[8:9])
    sz.decode(bytesdata[9:10])
    msgs = sz.decode(bytesdata[10:11])
    assert len(msgs) == 0
    msgs = sz.decode(bytesdata[11:12])
    msgs = sz.decode(bytesdata[12:13])
    msgs = sz.decode(bytesdata[13:14])
    msgs = sz.decode(bytesdata[14:15])
    msgs = sz.decode(bytesdata[15:16])
    assert len(msgs) == 1
    for msg in msgs:
        print("msgid %s,\nmsgdata %s\n" % msg)
    
    print("test sz.decode 4 complete msgs with one uncomplete msg")
    msgs = sz.decode(''.join((bytesdata[0:1], bytesdata[1:16], bytesdata, bytesdata, bytesdata, bytesdata[0:1])))
    assert len(msgs) == 4
    for msg in msgs:
        print("msgid %s,\n msgdata %s\n" % msg)
    print("the last  msg")
    sz.decode(bytesdata[1:2])
    sz.decode(bytesdata[2:3])
    sz.decode(bytesdata[3:4])
    sz.decode(bytesdata[4:5])
    sz.decode(bytesdata[5:6])
    sz.decode(bytesdata[6:7])
    sz.decode(bytesdata[7:8])
    sz.decode(bytesdata[8:9])
    sz.decode(bytesdata[9:10])
    msgs = sz.decode(bytesdata[10:])
    for msg in msgs:
        print("msgid %s,\n msgdata %s\n" % msg)
