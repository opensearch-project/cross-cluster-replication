package org.opensearch.replication.task.bulk

import kotlinx.coroutines.*
import org.opensearch.action.ActionFuture
import org.opensearch.action.support.master.AcknowledgedResponse
import org.opensearch.client.Client
import org.opensearch.cluster.service.ClusterService
import org.opensearch.persistent.AllocatedPersistentTask
import org.opensearch.persistent.PersistentTaskState
import org.opensearch.persistent.PersistentTasksService
import org.opensearch.replication.action.pause.PauseIndexReplicationAction
import org.opensearch.replication.action.pause.PauseIndexReplicationRequest
import org.opensearch.replication.action.resume.ResumeIndexReplicationAction
import org.opensearch.replication.action.resume.ResumeIndexReplicationRequest
import org.opensearch.replication.action.stop.StopIndexReplicationAction
import org.opensearch.replication.action.stop.StopIndexReplicationRequest
import org.opensearch.replication.task.index.IndexReplicationExecutor.Companion.log
import org.opensearch.replication.util.coroutineContext
import org.opensearch.tasks.TaskId
import org.opensearch.tasks.TaskManager
import org.opensearch.threadpool.ThreadPool


class BulkCrossClusterReplicationTask(
    id: Long, type: String?, action: String?, description: String?, parentTask: TaskId?,
    headers: Map<String, String>,
    threadPool: ThreadPool,
    protected val executor: String,
    protected val clusterService: ClusterService,
    protected val client: Client,
    val params: BulkParams
) :
    AllocatedPersistentTask(id, type, action, description, parentTask, headers) {

    private val overallTaskScope = CoroutineScope(threadPool.coroutineContext(executor))
    @Volatile private lateinit var taskManager: TaskManager
    fun init(persistentTasksService: PersistentTasksService, taskManager: TaskManager,
                      persistentTaskId: String, allocationId: Long, indexPattern: String) {
        super.init(persistentTasksService, taskManager, persistentTaskId, allocationId)
        this.taskManager = taskManager

    }

    fun run(initialState: PersistentTaskState? = null) {
        overallTaskScope.launch {

            log.info("Starting bulk cross cluster replication task")
            log.info("Pattern2 is ${params.patternName}")
//            log.info(getAllEligibleIndices(indexPattern).toList())

            val current_time = System.currentTimeMillis()
            val failures = mutableListOf<String>()
            val myList = getAllEligibleIndices(params.patternName)
            if (myList != null){
                myList.forEach{
                    val stopReplicationRequest =
                    try {
                        val response = BulkActionRequest(it)
                        log.info(response.actionGet())

                    }catch (e: Exception){
                        log.info("inside exception")
                        log.info("message ${e.message}")
                        e.message?.let { it1 -> failures.add(it1) }

                    } finally {

                        log.info("Finally block")
                        log.info("Parent task Id is ${taskManager.tasks}")
                    }

                }
            }
            markAsCompleted()
            log.info("************************")
            log.info(System.currentTimeMillis() - current_time)
        }
    }

    private fun BulkActionRequest(it: String): ActionFuture<AcknowledgedResponse>{
        return if (params.actionType == "STOP"){
            client.admin().cluster().execute(StopIndexReplicationAction.INSTANCE, StopIndexReplicationRequest(it))
        } else if (params.actionType == "PAUSE"){
            client.admin().cluster().execute(PauseIndexReplicationAction.INSTANCE, PauseIndexReplicationRequest(it))
        }else {
            client.admin().cluster().execute(ResumeIndexReplicationAction.INSTANCE, ResumeIndexReplicationRequest(it))
        }
    }



    private fun getAllEligibleIndices(indexPattern: String): Iterable<String> {
        //TODO remove system indices
        var currentIndices = clusterService.state().metadata().concreteAllIndices.asIterable().toList()
        currentIndices = currentIndices.filter { indexPattern.replace("*", ".*").toRegex().matches(it) }
        return currentIndices
    }




}