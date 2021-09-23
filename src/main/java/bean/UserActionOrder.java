package bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserActionOrder {
    private String id;
    private String action;
    private String orderId;
    private long ts;
}
