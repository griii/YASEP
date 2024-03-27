package ustc.nodb.IO;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PartitionResBuffer {
    // 定义一个List来作为Buffer的底层实现
    private final List<String> buffer;
    private final Lock lock = new ReentrantLock();

    private final String filePath;

    // 定义一个阈值，当Buffer中元素数量达到这个阈值时，触发写入文件操作
    private final int threshold = 1024;
    // 定义一个标志位，表示是否正在执行文件写入操作
    private volatile boolean isWriting = false;

    public PartitionResBuffer(String filePath) {
        this.filePath = filePath;
//        this.threshold = threshold;
        this.buffer = new ArrayList<>();
    }

    public void addList(List<String> elements){
        lock.lock();
        try {
            buffer.addAll(elements);
            // 检查Buffer是否已满，如果已满则执行写入文件操作
            if (buffer.size() >= threshold && !isWriting) {
                flushBufferToFile();
                buffer.clear();
            }
        } finally {
            lock.unlock();
        }
    }

    // 向Buffer中添加元素的方法
    public void add(String element) {
        lock.lock();
        try {
            buffer.add(element);
            // 检查Buffer是否已满，如果已满则执行写入文件操作
            if (buffer.size() >= threshold) {
                flushBufferToFile();
                buffer.clear();
            }
        } finally {
            lock.unlock();
        }
    }

    public void flush() {
        lock.lock();
        try {
            // 检查Buffer是否已满，如果已满则执行写入文件操作
            if (buffer.size() >= 0) {
                flushBufferToFile();
                buffer.clear();
            }
        } finally {
            lock.unlock();
        }
    }

    // 将Buffer中的内容异步写入文件的方法
    public void flushBufferToFile() {

        isWriting = true;
        List<String> ioBuffer = new ArrayList<>(buffer);
        new Thread(() -> {
            try {
                // 锁定Buffer，防止在写入文件期间有新的写入操作
                writeFileContent(ioBuffer);
            } finally {
                // 释放锁，允许后续的写入操作
                isWriting = false;
            }
        }).start();
    }


    private void writeFileContent(List<String> ioBuffer) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
            for (String line : ioBuffer) {
                // 写入每一行
                writer.write(line);
                writer.newLine(); // 添加换行符，确保每条记录都在新的一行
            }
        } catch (IOException e) {
            // 处理I/O异常，例如记录日志或者抛出自定义异常
            e.printStackTrace();
        }
    }

}