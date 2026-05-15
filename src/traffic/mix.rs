//! 混合流量生成器
//!
//! 将多种流量生成器的输出按比例混合，构造复合工作负载。
//! 典型用法：80% mice flow + 20% elephant flow，
//! 或 Incast 背景流 + AllToAll 前台流。
//!
//! 混合时自动为 flow_id 去重，保证全局唯一。

use super::FlowDesc;

/// 混合流量组件描述
#[derive(Debug, Clone)]
pub struct MixComponent {
    /// 流量描述列表（通常由其他生成器预先 `generate()` 产出）
    pub flows: Vec<FlowDesc>,
    /// 该组件在混合中的权重（仅用于参考比例，不强制截断）
    pub weight: f64,
}

/// 混合流量生成器
#[derive(Debug, Clone)]
pub struct Mix {
    pub components: Vec<MixComponent>,
}

impl Mix {
    /// 创建空的混合器
    pub fn new() -> Self {
        Self { components: Vec::new() }
    }

    /// 添加一个组件
    pub fn add(mut self, flows: Vec<FlowDesc>, weight: f64) -> Self {
        self.components.push(MixComponent { flows, weight });
        self
    }

    /// 生成混合后的流量列表
    ///
    /// 行为：
    /// - 将所有组件的流按 `weight` 降序排列（权重大的在前）
    /// - 重新分配全局唯一的 `flow_id`
    /// - 返回平铺后的 `Vec<FlowDesc>`
    pub fn generate(&self) -> Vec<FlowDesc> {
        let mut all = Vec::new();
        // 按权重降序合并（权重大的优先，模拟 foreground traffic）
        let mut comps: Vec<_> = self.components.iter().collect();
        comps.sort_by(|a, b| b.weight.partial_cmp(&a.weight).unwrap());

        let mut next_fid: u32 = 0;
        for comp in comps {
            for mut f in comp.flows.clone() {
                f.flow_id = next_fid;
                next_fid += 1;
                all.push(f);
            }
        }
        all
    }
}

impl Default for Mix {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mix_combines_and_renumbers() {
        let c1 = vec![
            FlowDesc { flow_id: 0, src: 0, dst: 1, bytes: 100, start_time_ns: 0 },
            FlowDesc { flow_id: 1, src: 0, dst: 2, bytes: 100, start_time_ns: 0 },
        ];
        let c2 = vec![
            FlowDesc { flow_id: 0, src: 1, dst: 0, bytes: 1000, start_time_ns: 100 },
        ];
        let mix = Mix::new().add(c1, 1.0).add(c2, 2.0);
        let flows = mix.generate();
        assert_eq!(flows.len(), 3);
        // flow_id 应全局唯一：0, 1, 2
        assert_eq!(flows[0].flow_id, 0);
        assert_eq!(flows[1].flow_id, 1);
        assert_eq!(flows[2].flow_id, 2);
        // 权重大的 c2 排在前面
        assert_eq!(flows[0].bytes, 1000);
    }
}
