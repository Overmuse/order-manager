use chrono::Utc;
use futures::stream::StreamExt;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_util::time::delay_queue::DelayQueue;
use tracing::{debug, info};
use trading_base::PositionIntent;

pub struct IntentScheduler {
    scheduled_intents: DelayQueue<PositionIntent>,
    receiver: UnboundedReceiver<PositionIntent>,
    sender: UnboundedSender<PositionIntent>,
}

impl IntentScheduler {
    pub fn new(
        sender: UnboundedSender<PositionIntent>,
        receiver: UnboundedReceiver<PositionIntent>,
    ) -> Self {
        Self {
            scheduled_intents: DelayQueue::new(),
            sender,
            receiver,
        }
    }

    #[tracing::instrument(skip(self))]
    pub async fn run(mut self) {
        info!("Starting IntentScheduler");
        loop {
            tokio::select! {
                intent = self.receiver.recv() => {
                    if let Some(intent) = intent {
                        debug!("Intent received for scheduling");
                        self.schedule(intent)
                    }
                },
                intent = self.scheduled_intents.next(), if !self.scheduled_intents.is_empty() => {
                    if let Some(Ok(intent)) = intent {
                        debug!("Scheduled intent triggered");
                        self.sender.send(intent.into_inner()).unwrap()
                    }
                }
            }
        }
    }

    #[tracing::instrument(skip(self, intent), fields(id = %intent.id))]
    fn schedule(&mut self, mut intent: PositionIntent) {
        let trigger_time = intent
            .after
            .take() // We take the field so that there's no longer an `after` condition
            .expect("schedule_position_intent called with intent lacking `after` field");
        debug!("Scheduling intent for {}", trigger_time);
        self.scheduled_intents
            .insert(intent, (trigger_time - Utc::now()).to_std().unwrap());
    }
}
