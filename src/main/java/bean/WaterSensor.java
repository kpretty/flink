package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Project: flink<br/>
 * Package: bean<br/>
 * Version: 1.0<br/>
 * Author: wjun<br/>
 * Description:
 * <br/>
 * Created by hc on 2021/09/08 17:58<br/>
 * © 1996 - 2021 Zhejiang Hong Cheng Computer Systems Co., Ltd.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    private String id;  //传感器id
    private long ts;    //时间戳
    private double vc;  //水位
}
