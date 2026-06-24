use std::collections::HashMap;
use std::sync::atomic::{AtomicU16, Ordering};

use bytes::Bytes;
use tokio::sync::mpsc;
use tracing::instrument;
use crate::packet::QoS;
use crate::packet::publish::PublishPacket;

pub type ClientId = String;
pub type PacketId = u16;

#[derive(Debug, Clone)]
pub struct RoutedMessage {
    pub publish: PublishPacket,
    pub packet_id: Option<u16>,
}

#[derive(Debug)]
pub enum ClientCommand {
    Subscribe {
        topic_filter: String,
        qos: QoS,
    },
    UnsubscribeAll,
    Publish {
        publish: PublishPacket,
    },
    PubAck {
        packet_id: PacketId,
    },
}

pub struct SubscriptionEntry {
    pub client_id: ClientId,
    pub topic_filter: String,
    pub qos: QoS,
    pub sender: mpsc::Sender<RoutedMessage>,
}

pub struct Router {
    subscriptions: HashMap<ClientId, Vec<SubscriptionEntry>>,
    senders: HashMap<ClientId, mpsc::Sender<RoutedMessage>>,
    retained: HashMap<String, PublishPacket>,
    next_packet_id: AtomicU16,
}

impl Router {
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            senders: HashMap::new(),
            retained: HashMap::new(),
            next_packet_id: AtomicU16::new(1),
        }
    }

    pub fn next_packet_id(&self) -> PacketId {
        self.next_packet_id.fetch_add(1, Ordering::Relaxed)
    }

    #[instrument(skip(self, sender), fields(client_id = %client_id))]
    pub fn register_client(&mut self, client_id: ClientId, sender: mpsc::Sender<RoutedMessage>) {
        self.senders.insert(client_id.clone(), sender);
        self.subscriptions.entry(client_id).or_default();
    }

    #[instrument(skip(self), fields(client_id = %client_id))]
    pub fn remove_client(&mut self, client_id: &str) {
        self.subscriptions.remove(client_id);
        self.senders.remove(client_id);
    }

    #[instrument(skip(self), fields(client_id = %client_id, topic = %topic_filter))]
    pub fn subscribe(
        &mut self,
        client_id: &str,
        topic_filter: String,
        qos: QoS,
        sender: mpsc::Sender<RoutedMessage>,
    ) -> QoS {
        let granted_qos = qos;

        self.senders.insert(client_id.to_owned(), sender.clone());

        let entries = self.subscriptions.entry(client_id.to_owned()).or_default();
        entries.retain(|e| e.topic_filter != topic_filter);
        entries.push(SubscriptionEntry {
            client_id: client_id.to_owned(),
            topic_filter: topic_filter.clone(),
            qos: granted_qos,
            sender,
        });

        granted_qos
    }

    #[instrument(skip(self, publish), fields(topic = %publish.topic))]
    pub fn publish(&mut self, publish: PublishPacket) -> Vec<(ClientId, Option<PacketId>)> {
        if publish.retain {
            self.retained.insert(publish.topic.clone(), publish.clone());
        }

        let mut ack_targets = Vec::new();
        let effective_qos = publish.qos;

        for entries in self.subscriptions.values() {
            for entry in entries {
                if !topic_matches(&entry.topic_filter, &publish.topic) {
                    continue;
                }

                let delivery_qos = std::cmp::min(effective_qos.as_u8(), entry.qos.as_u8());
                let delivery_qos = match delivery_qos {
                    0 => QoS::AtMostOnce,
                    _ => QoS::AtLeastOnce,
                };

                let outbound_id = if delivery_qos == QoS::AtLeastOnce {
                    Some(self.next_packet_id())
                } else {
                    None
                };

                let outbound = PublishPacket {
                    dup: false,
                    qos: delivery_qos,
                    retain: false,
                    topic: publish.topic.clone(),
                    packet_id: outbound_id,
                    payload: publish.payload.clone(),
                };

                let msg = RoutedMessage {
                    publish: outbound,
                    packet_id: outbound_id,
                };

                if entry.sender.try_send(msg).is_ok() && outbound_id.is_some() {
                    ack_targets.push((entry.client_id.clone(), outbound_id));
                }
            }
        }

        ack_targets
    }

    pub fn retained_messages_matching(&self, topic_filter: &str) -> Vec<PublishPacket> {
        self.retained
            .iter()
            .filter(|(topic, _)| topic_matches(topic_filter, topic))
            .map(|(_, msg)| msg.clone())
            .collect()
    }

    pub fn subscription_count(&self) -> usize {
        self.subscriptions.values().map(|v| v.len()).sum()
    }

    pub fn client_count(&self) -> usize {
        self.senders.len()
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}

/// MQTT topic filter matching with `+` (single level) and `#` (multi-level) wildcards.
pub fn topic_matches(filter: &str, topic: &str) -> bool {
    let filter_parts: Vec<&str> = filter.split('/').collect();
    let topic_parts: Vec<&str> = topic.split('/').collect();
    match_topic_parts(&filter_parts, &topic_parts)
}

fn match_topic_parts(filter: &[&str], topic: &[&str]) -> bool {
    if filter.is_empty() {
        return topic.is_empty();
    }

    if filter[0] == "#" {
        return filter.len() == 1;
    }

    if topic.is_empty() {
        return false;
    }

    if filter[0] == "+" || filter[0] == topic[0] {
        return match_topic_parts(&filter[1..], &topic[1..]);
    }

    false
}

pub fn build_publish(topic: &str, payload: impl Into<Bytes>, qos: QoS, retain: bool) -> PublishPacket {
    PublishPacket {
        dup: false,
        qos,
        retain,
        topic: topic.to_owned(),
        packet_id: if qos == QoS::AtMostOnce {
            None
        } else {
            Some(1)
        },
        payload: payload.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_match() {
        assert!(topic_matches("home/living/temp", "home/living/temp"));
        assert!(!topic_matches("home/living/temp", "home/kitchen/temp"));
    }

    #[test]
    fn single_level_wildcard() {
        assert!(topic_matches("home/+/temp", "home/living/temp"));
        assert!(topic_matches("home/+/temp", "home/kitchen/temp"));
        assert!(!topic_matches("home/+/temp", "home/living/humidity"));
    }

    #[test]
    fn multi_level_wildcard() {
        assert!(topic_matches("home/#", "home/living/temp"));
        assert!(topic_matches("home/#", "home"));
        assert!(!topic_matches("home/#", "office/desk"));
    }
}
