//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//

// Greedy Algorithm
// Loads up the first machine

#include <set>
#include "Scheduler.hpp"

static bool migrating = false;
static unsigned active_machines;

// Active and Inactive Machines
set<MachineId_t> on_machines; //Using sets because easier to insert and erase
set<MachineId_t> off_machines;

void Scheduler::Init() {
    // Find the parameters of the clusters
    // Get the total number of machines
    // For each machine:
    //      Get the type of the machine
    //      Get the memory of the machine
    //      Get the number of CPUs
    //      Get if there is a GPU or not
    // 
    SimOutput("Scheduler::Init(): Total number of machines is " + to_string(Machine_GetTotal()), 3);
    SimOutput("Scheduler::Init(): Initializing scheduler", 1);
    for (unsigned i = 0; i < Machine_GetTotal(); i++) {
        //Add machines the total machine vector
        machines.push_back(MachineId_t(i));
        //By default machines are on so add them to on_machines set
        on_machines.insert(MachineId_t(i));
    }
    //Update active machines
    active_machines = Machine_GetTotal();
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Update your data structure. The VM now can receive new tasks
}

//Method that checks memory, cpu availability and gpu requirements
bool CheckRequirements(MachineId_t machine_ID, TaskId_t task_id){
    bool gpu_capable = IsTaskGPUCapable(task_id);
    unsigned task_mem = GetTaskMemory(task_id);
    VMType_t req_vm = RequiredVMType(task_id);
    CPUType_t req_cpu_type = RequiredCPUType(task_id);
    unsigned int mem_with_task = task_mem + Machine_GetInfo(machine_ID).memory_used;
    unsigned int cores_with_task = 1 + Machine_GetInfo(machine_ID).active_tasks;
    return mem_with_task <= Machine_GetInfo(machine_ID).memory_size    
        && cores_with_task <= Machine_GetInfo(machine_ID).num_cpus 
        // Task can run on machine even the machine doesn't have a GPU
        && (Machine_GetInfo(machine_ID).gpus == gpu_capable || (Machine_GetInfo(machine_ID).gpus && !gpu_capable));
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // Get the task parameters
    bool gpu_capable = IsTaskGPUCapable(task_id);
    unsigned task_mem = GetTaskMemory(task_id);
    VMType_t req_vm = RequiredVMType(task_id);
    SLAType_t req_sla = RequiredSLA(task_id);
    CPUType_t req_cpu_type = RequiredCPUType(task_id);
    // Loop through VMs to see if there is a compatible one
    for(auto &vm_id: vms){
        if(VM_GetInfo(vm_id).vm_type == req_vm){
            MachineId_t machine_id = VM_GetInfo(vm_id).machine_id;
            if(Machine_GetInfo(machine_id).s_state != S5 && VM_GetInfo(vm_id).vm_type == req_vm && Machine_GetInfo(machine_id).cpu == req_cpu_type 
                && CheckRequirements(machine_id, task_id)){
                //Task finally can be put on a VM and machine with ALL requirements
                VM_AddTask(vm_id, task_id, LOW_PRIORITY);
                return;
            }
        }
    }
    // Haven't found active VM so loop through active machines that meet requirements
    for(auto &machine_id : on_machines){
        // Find machine that is active and compatible CPU and meets other requirements
        if(Machine_GetInfo(machine_id).s_state != S5 && Machine_GetCPUType(machine_id) == req_cpu_type && CheckRequirements(machine_id, task_id)){
            // Found Machine, Now create VM
            VMId_t vm = VM_Create(req_vm, req_cpu_type);
            vms.push_back(vm);
            VM_Attach(vm, machines[machine_id]); 
            VM_AddTask(vm, task_id, LOW_PRIORITY);
            return;
        }
    }
    // Didn't find active machines that meet requirements
    // No CPU was found in the ON machines so loop through OFF machines
    for (auto it = off_machines.begin(); it != off_machines.end(); ) {
        int machine_id = *it;
        if(Machine_GetCPUType(machine_id) == req_cpu_type && CheckRequirements(machine_id, task_id)){
            // Found Machine 
            Machine_SetState(machine_id, S0); // Power up the machine
            return; // Return so StateChangeComplete can be called
        } else {
            ++it; // Move to the next machine
            }
    }
}




void Scheduler::PeriodicCheck(Time_t now) {
    // This method should be called from SchedulerCheck()
    // SchedulerCheck is called periodically by the simulator to allow you to monitor, make decisions, adjustments, etc.
    // Unlike the other invocations of the scheduler, this one doesn't report any specific event
    // Recommendation: Take advantage of this function to do some monitoring and adjustments as necessary
    // Checks if machines that are active if they are running tasks
    if (now > 7000)
    {
        for(auto &machine : on_machines){
            if(Machine_GetInfo(machine).active_tasks==0 && Machine_GetInfo(machine).s_state!=S5){
                // Shuts machine down if no active tasks
                Machine_SetState(machine, S5);
            }
        }
    }
}

void Scheduler::Shutdown(Time_t time) {
    // Do your final reporting and bookkeeping here.
    // Report about the total energy consumed
    // Report about the SLA compliance
    // Shutdown everything to be tidy :-)
    for(auto & vm: vms) {
        VM_Shutdown(vm);
    }
    SimOutput("SimulationComplete(): Finished!", 4);
    SimOutput("SimulationComplete(): Time is " + to_string(time), 4);
}

void Scheduler::TaskComplete(Time_t now, TaskId_t task_id) {
    // Do any bookkeeping necessary for the data structures
    // Decide if a machine is to be turned off, slowed down, or VMs to be migrated according to your policy
    // This is an opportunity to make any adjustments to optimize performance/energy
    SimOutput("Scheduler::TaskComplete(): Task " + to_string(task_id) + " is complete at " + to_string(now), 4);
}

// Public interface below

static Scheduler Scheduler;

void InitScheduler() {
    SimOutput("InitScheduler(): Initializing scheduler", 4);
    Scheduler.Init();
}

void HandleNewTask(Time_t time, TaskId_t task_id) {
    SimOutput("HandleNewTask(): Received new task " + to_string(task_id) + " at time " + to_string(time), 4);
    Scheduler.NewTask(time, task_id);
}

void HandleTaskCompletion(Time_t time, TaskId_t task_id) {
    SimOutput("HandleTaskCompletion(): Task " + to_string(task_id) + " completed at time " + to_string(time), 4);
    Scheduler.TaskComplete(time, task_id);
}

void MemoryWarning(Time_t time, MachineId_t machine_id) {
    // The simulator is alerting you that machine identified by machine_id is overcommitted
    SimOutput("MemoryWarning(): Overflow at " + to_string(machine_id) + " was detected at time " + to_string(time), 0);
}

void MigrationDone(Time_t time, VMId_t vm_id) {
    // The function is called on to alert you that migration is complete
    SimOutput("MigrationDone(): Migration of VM " + to_string(vm_id) + " was completed at time " + to_string(time), 4);
    Scheduler.MigrationComplete(time, vm_id);
    migrating = false;
}

void SchedulerCheck(Time_t time) {
    // This function is called periodically by the simulator, no specific event
    SimOutput("SchedulerCheck(): SchedulerCheck() called at " + to_string(time), 4);
    Scheduler.PeriodicCheck(time);
}

void SimulationComplete(Time_t time) {
    // This function is called before the simulation terminates Add whatever you feel like.
    cout << "SLA violation report" << endl;
    cout << "SLA0: " << GetSLAReport(SLA0) << "%" << endl;
    cout << "SLA1: " << GetSLAReport(SLA1) << "%" << endl;
    cout << "SLA2: " << GetSLAReport(SLA2) << "%" << endl;     // SLA3 do not have SLA violation issues
    cout << "Total Energy " << Machine_GetClusterEnergy() << "KW-Hour" << endl;
    cout << "Simulation run finished in " << double(time)/1000000 << " seconds" << endl;
    SimOutput("SimulationComplete(): Simulation finished at time " + to_string(time), 4);
    
    Scheduler.Shutdown(time);
}

void SLAWarning(Time_t time, TaskId_t task_id) {
    
}

void StateChangeComplete(Time_t time, MachineId_t machine_id) {
    // Called in response to an earlier request to change the state of a machine
    // Removes the machine from its original list and gets added to the oppsite one after chaning state
    if (Machine_GetInfo(machine_id).s_state == S0) {
        on_machines.insert(machine_id);
        off_machines.erase(machine_id);;
        active_machines++;

    }
    if (Machine_GetInfo(machine_id).s_state == S5) {
        on_machines.erase(machine_id);
        off_machines.insert(machine_id);
        active_machines--;
    }
}

