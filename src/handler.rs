use std::sync::Arc;

use prosody::consumer::message::{ConsumerMessage, MessageContext};
use prosody::consumer::MessageHandler;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::PyAnyMethods;
use pyo3::{Bound, PyAny, PyErr, PyObject, PyResult, Python};
use pythonize::pythonize;
use tracing::error;

use crate::message::{Context, Message};

const HANDLER_CLASS_NAME: &str = "AbstractMessageHandler";

#[derive(Clone)]
pub struct PythonHandler {
    handler: Arc<PyObject>,
}

impl PythonHandler {
    pub fn new(handler: &Bound<PyAny>) -> PyResult<Self> {
        let abstract_handler_class = handler
            .py()
            .import_bound("prosody")?
            .getattr(HANDLER_CLASS_NAME)?;

        if !handler.is_instance(&abstract_handler_class)? {
            return Err(PyTypeError::new_err(format!(
                "handler must be a subclass of {HANDLER_CLASS_NAME}"
            )));
        };

        let handler = Arc::new(handler.getattr("handle")?.unbind());
        Ok(Self { handler })
    }
}

impl MessageHandler for PythonHandler {
    type Error = PyErr;

    async fn handle(
        &self,
        context: MessageContext,
        message: ConsumerMessage,
    ) -> Result<(), Self::Error> {
        let topic = message.topic().into();
        let partition = message.partition();
        let offset = message.offset();
        let (key, payload, uncommitted) = message.into_inner();

        Python::with_gil(|py| {
            let payload = match pythonize(py, &payload) {
                Ok(payload) => payload,
                Err(error) => {
                    error!(
                        %topic, %partition, %offset, %key,
                        "unable to decode payload: {error:#}; discarding message"
                    );
                    uncommitted.commit();

                    return Ok(());
                }
            };

            let context = Context(context);
            let message = Message {
                topic,
                partition,
                offset,
                key,
                payload,
            };

            self.handler.call1(py, (context, message))?;
            uncommitted.commit();

            Ok(())
        })
    }
}
