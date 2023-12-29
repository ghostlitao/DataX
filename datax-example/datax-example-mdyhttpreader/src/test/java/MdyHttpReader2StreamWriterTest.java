import com.alibaba.datax.example.ExampleContainer;
import com.alibaba.datax.example.util.PathUtil;
import org.junit.Test;

/**
 * {@code Author} ghostlitao
 * {@code Date}  2023-12-29
 */

public class MdyHttpReader2StreamWriterTest {
    @Test
    public void testStreamReader2StreamWriter() {
        String path = "/mdyhttpreader2stream.json";
        String jobPath = PathUtil.getAbsolutePathFromClassPath(path);
        ExampleContainer.start(jobPath);
    }
}
