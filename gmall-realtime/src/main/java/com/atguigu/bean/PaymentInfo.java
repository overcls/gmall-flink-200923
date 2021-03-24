package com.atguigu.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * Date:2021/3/22
 * Description:
 */
@Data
public class PaymentInfo {
    Long id;
    Long order_id;
    Long user_id;
    BigDecimal total_amount;
    String subject;
    String payment_type;
    String create_time;
    String callback_time;
}
