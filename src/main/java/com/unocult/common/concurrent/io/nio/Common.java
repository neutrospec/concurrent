package com.unocult.common.concurrent.io.nio;

import java.nio.ByteBuffer;

public class Common {
	public static int fillBuffer(ByteBuffer target, ByteBuffer source) {
		if (source == null)
			return 0;
		int n = Math.min(source.remaining(), target.remaining());
		if (n == 0)
			return 0;
		if ( n < 16) {
			for (int i = 0; i < n; i++) {
				target.put(source.get());
			}
		} else if (target.hasArray() && source.hasArray()) {
			int sourcePosition = source.position();
			int targetPosition = target.position();
			System.arraycopy(source.array(), sourcePosition + source.arrayOffset(), target.array(), targetPosition + target.arrayOffset(), n);
			source.position(sourcePosition + n);
			target.position(targetPosition + n);
		} else {
            fillDirectBuffer(target, source);
        }
		return n;
	}
	public static int fillDirectBuffer(ByteBuffer target, ByteBuffer source) {
        int n = Math.min(source.remaining(), target.remaining());
        if (source.remaining() <= n) {
            target.put(source);
        } else {
            int realLimit = source.limit();
            source.limit(source.position() + n);
            target.put(source);
            source.limit(realLimit);
        }
        return n;
	}
}
