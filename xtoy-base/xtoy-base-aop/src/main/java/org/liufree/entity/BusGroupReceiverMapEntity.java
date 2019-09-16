package org.liufree.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BusGroupReceiverMapEntity {
    private Long id;
    private Long groupId;
    private Long receiverId;
}
