package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    private String id;  //传感器id
    private long ts;    //时间戳
    private double vc;  //水位
}
