import com.dianping.DianPingApplication;
import com.dianping.service.impl.ShopServiceImpl;
import jakarta.annotation.Resource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * @Description: 测试
 * @Author: zhao
 * Created: 2025/4/23 - 21:24
 */

@SpringBootTest(classes = DianPingApplication.class)
public class DianPingApplicationTests {

    @Resource
    private ShopServiceImpl shopService;

    @Test
    void testSaveShop() {
        shopService.saveShop2Redis(1L, 10L);
    }
}
