

# TODO: switch to base class and inherit for each workflow
class WorkflowHandler():
    def __init__(self):
        pass

    @staticmethod
    def run_workflow_a(swarm_client, input_data, persist):
        # TODO: create(if not persist) or retrieve required docker containers

        # TODO: Add containers to map with component_name,ip,port if persist

        # TODO: Send request to component in order one-by-one and transform result
        # as required for next component

        # TODO: terminate containers if not persist
        pass