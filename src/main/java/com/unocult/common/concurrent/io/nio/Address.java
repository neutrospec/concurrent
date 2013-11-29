package com.unocult.common.concurrent.io.nio;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;

public class Address {
	private String host;
	private byte[] ip = new byte[4];
	private int port;
	private InetSocketAddress inetSocketAddress;
    private int hash = -1;
	
	public Address() {
	}
	public Address(String host, int port) {
		this(new InetSocketAddress(host, port));
	}
	public Address(InetSocketAddress inet) {
		this.ip = inet.getAddress().getAddress();
		this.port = inet.getPort();
		this.inetSocketAddress = inet;
	}
    public InetSocketAddress getInetSocketAddress() {
        return inetSocketAddress;
    }
	public InetAddress getInetAddress() {
		if (inetSocketAddress == null)
			inetSocketAddress = new InetSocketAddress(host, port);
		return inetSocketAddress.getAddress();
	}
	public void readData(DataInput dataIn) throws IOException {
		dataIn.readFully(ip);
		port = dataIn.readInt();
	}
	public void writeData(DataOutput dataOut) throws IOException {
		dataOut.write(ip);
		dataOut.writeInt(port);
	}
	public String getHost() { 
		if (host == null)
			setHost();
		return host; 
	}
	private void setHost() {
		host =   (ip[0] & 0xff) + "." + (ip[1] & 0xff) + "." + (ip[2] & 0xff) + "."+ (ip[3] & 0xff);
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public String toString() {
		return getHost() + ":" + port;
	}
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null)
            return false;
        if (!(o instanceof Address))
            return false;
        final Address address = (Address) o;
        if (hashCode() != address.hashCode()) {
            return false;
        }
        return port == address.port && Arrays.equals(ip, address.ip);
    }

    @Override
    public int hashCode() {
        if (hash == -1)
            setHashCode();
        return hash;
    }

    private void setHashCode() {
        this.hash = hash(ip) * 29 + port;
    }

    private int hash(byte[] bytes) {
        int hash = 0;
        for (byte b : bytes) {
            hash = (hash * 29) + b;
        }
        return hash;
    }

}
