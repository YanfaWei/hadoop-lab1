import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class HdfsClient {
    private static FileSystem fs;
    private static FileSystem localFs;
    private static final Scanner SC = new Scanner(System.in);

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        fs = FileSystem.get(conf);
        localFs = FileSystem.getLocal(conf);
        while (true) {
            System.out.println("\n1上传 2下载 3显示内容 4文件信息 5递归列出");
            System.out.println("6创建/删除文件 7创建/删除目录 8追加内容 9删除文件 10移动 0退出");
            System.out.print("选择: ");
            switch (SC.nextLine().trim()) {
                case "1": upload(); break;
                case "2": download(); break;
                case "3": cat(); break;
                case "4": info(new Path(read("HDFS文件路径: "))); break;
                case "5": listRecursive(); break;
                case "6": createOrDeleteFile(); break;
                case "7": createOrDeleteDir(); break;
                case "8": appendContent(); break;
                case "9": fs.delete(new Path(read("要删除的HDFS文件: ")), false); break;
                case "10":
                    fs.rename(new Path(read("源路径: ")), new Path(read("目的路径: ")));
                    break;
                case "0": fs.close(); return;
                default: System.out.println("无效选项");
            }
        }
    }

    private static void upload() throws IOException {
        Path local = new Path(read("本地文件绝对路径: "));
        Path dst = new Path(read("HDFS目标路径: "));
        if (dst.getParent() != null) fs.mkdirs(dst.getParent());
        if (fs.exists(dst)) {
            String op = read("已存在。1覆盖 2追加到末尾: ");
            if ("1".equals(op)) fs.copyFromLocalFile(false, true, local, dst);
            else {
                try (InputStream in = new FileInputStream(local.toString());
                     FSDataOutputStream out = fs.append(dst)) {
                    copy(in, out);
                }
            }
        } else fs.copyFromLocalFile(false, true, local, dst);
        System.out.println("上传完成");
    }

    private static void download() throws IOException {
        Path src = new Path(read("HDFS文件路径: "));
        Path dst = new Path(read("本地绝对路径: "));
        Path parent = dst.getParent() == null ? new Path(System.getProperty("user.dir")) : dst.getParent();
        localFs.mkdirs(parent);
        String base = dst.getName();
        int i = 1;
        while (localFs.exists(dst)) dst = new Path(parent, base + "_" + i++);
        fs.copyToLocalFile(false, src, dst, true);
        System.out.println("已下载到 " + dst);
    }

    private static void cat() throws IOException {
        try (FSDataInputStream in = fs.open(new Path(read("HDFS文件路径: ")));
             BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
            String line;
            while ((line = br.readLine()) != null) System.out.println(line);
        }
    }

    private static void info(Path path) throws IOException {
        FileStatus st = fs.getFileStatus(path);
        String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(st.getModificationTime()));
        System.out.println(st.getPermission() + "\t" + st.getLen() + "\t" + time + "\t" + st.getPath());
    }

    private static void listRecursive() throws IOException {
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path(read("HDFS目录: ")), true);
        while (it.hasNext()) info(it.next().getPath());
    }

    private static void createOrDeleteFile() throws IOException {
        Path path = new Path(read("HDFS文件路径: "));
        if ("1".equals(read("1创建 2删除: "))) {
            if (path.getParent() != null) fs.mkdirs(path.getParent());
            fs.create(path, true).close();
            System.out.println("已创建");
        } else {
            fs.delete(path, false);
            System.out.println("已删除");
        }
    }

    private static void createOrDeleteDir() throws IOException {
        Path path = new Path(read("HDFS目录路径: "));
        if ("1".equals(read("1创建 2删除: "))) {
            fs.mkdirs(path);
            System.out.println("已创建");
            return;
        }
        if (!fs.exists(path)) {
            System.out.println("目录不存在");
            return;
        }
        if (fs.listStatus(path).length == 0) {
            fs.delete(path, false);
            System.out.println("空目录已删除");
        } else System.out.println("目录非空，未删除");
    }

    private static void appendContent() throws IOException {
        Path path = new Path(read("HDFS文件路径: "));
        byte[] extra = (read("要追加的内容: ") + "\n").getBytes();
        if ("1".equals(read("1追加到开头 2追加到末尾: "))) {
            byte[] old = readAll(path);
            try (FSDataOutputStream out = fs.create(path, true)) {
                out.write(extra);
                out.write(old);
            }
        } else {
            try (FSDataOutputStream out = fs.append(path)) {
                out.write(extra);
            }
        }
        System.out.println("追加完成");
    }

    private static byte[] readAll(Path path) throws IOException {
        try (FSDataInputStream in = fs.open(path); ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            copy(in, bos);
            return bos.toByteArray();
        }
    }

    private static void copy(InputStream in, OutputStream out) throws IOException {
        byte[] buf = new byte[4096];
        int n;
        while ((n = in.read(buf)) > 0) out.write(buf, 0, n);
    }

    private static String read(String tip) {
        while (true) {
            System.out.println(tip);
            System.out.flush();
            String line = SC.nextLine();
            if (line == null) continue;
            line = line.trim();
            if (!line.isEmpty()) return line;
            System.out.println("空行已忽略，请重新输入");
        }
    }
}

