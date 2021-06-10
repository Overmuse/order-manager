use chrono::Utc;
use futures::stream::StreamExt;
use position_intents::PositionIntent;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_util::time::delay_queue::DelayQueue;
use tracing::{info, trace};

pub(super) struct IntentScheduler {
    scheduled_intents: DelayQueue<PositionIntent>,
    receiver: UnboundedReceiver<PositionIntent>,
    sender: UnboundedSender<PositionIntent>,
}

impl IntentScheduler {
    pub(super) fn new(
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
    pub(super) async fn run(mut self) {
        info!("Starting IntentScheduler");
        loop {
            tokio::select! {
                    intent = self.receiver.recv() => {
                        if let Some(intent) = intent {
                            trace!("Intent received for scheduling: {:?}", intent);
                            self.schedule(intent)
                        }
                    },
                    intent = self.scheduled_intents.next(), if !self.scheduled_intents.is_empty() => {
                        if let Some(Ok(intent)) = intent {
                            trace!("Scheduled intent triggered: {:?}", intent);
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
        trace!("Scheduling intent for {}", trigger_time);
        self.scheduled_intents
            .insert(intent, (trigger_time - Utc::now()).to_std().unwrap());
    }
}
