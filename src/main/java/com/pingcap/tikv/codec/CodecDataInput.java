/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.codec;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;


public class CodecDataInput implements DataInput {
    private DataInputStream s;
    private int size;
    public CodecDataInput(byte[] buf) {
        size = buf.length;
        s = new DataInputStream(new ByteArrayInputStream(buf));
    }
    @Override
    public void readFully(byte[] b) {
        try {
            s.readFully(b);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void readFully(byte[] b, int off, int len) {
        try {
            s.readFully(b, off, len);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int skipBytes(int n) {
        try {
            return s.skipBytes(n);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean readBoolean() {
        try {
            return s.readBoolean();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte readByte() {
        try {
            return s.readByte();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readUnsignedByte() {
        try {
            return s.readUnsignedByte();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public short readShort() {
        try {
            return s.readShort();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readUnsignedShort() {
        try {
            return s.readUnsignedShort();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public char readChar() {
        try {
            return s.readChar();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int readInt() {
        try {
            return s.readInt();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long readLong() {
        try {
            return s.readLong();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public float readFloat() {
        try {
            return s.readFloat();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public double readDouble() {
        try {
            return s.readDouble();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String readLine() {
        try {
            return s.readLine();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String readUTF() {
        try {
            return s.readUTF();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean eof() {
        return false;
    }
    public int size() { return size;}
}
