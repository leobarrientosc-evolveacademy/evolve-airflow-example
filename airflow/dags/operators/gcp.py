from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.utils.context import Context

class GenerativeImageHook(GoogleBaseHook):
    def _hello():
        print("Hello")

class GenerativeModelGenerateImageOperator(GoogleCloudBaseOperator):

    template_fields = ("location", "project_id", "prompt")
    
    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        prompt: str,
        pretrained_model: str = "text-bison",
        gcp_conn_id: str = "google_cloud_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.prompt = prompt
        self.pretrained_model = pretrained_model
        self.gcp_conn_id = gcp_conn_id
        
    def execute(self, context: Context):
        
        self.log.info("Submitting prompt")
        
        self.hook = GenerativeImageHook(
            gcp_conn_id=self.gcp_conn_id
        )
        
        print(self.hook._get_access_token())
        
        prompt=self.prompt
        #self.log.info("Model response: %s", response)
        self.xcom_push(context, key="prompt_request", value=self.prompt)

        return {}