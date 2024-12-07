//
//  Scheduler.cpp
//  CloudSim
//
//  Created by ELMOOTAZBELLAH ELNOZAHY on 10/20/24.
//
#include <list>
#include <map>
#include <vector>
#include <unordered_set> 
#include <set>
#include "Scheduler.hpp"

static bool migrating = false;
static unsigned active_machines;

// Active and Inactive Machines
set<MachineId_t> on_machines;
set<MachineId_t> off_machines;
vector<TaskId_t> total_task;

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
        machines.push_back(MachineId_t(i));
        //Machine_SetState(MachineId_t(i), S0);
        //SimOutput("machine id: " + MachineId_t(i), 1);
        SimOutput("machine p_state: " + to_string(Machine_GetInfo(MachineId_t(i)).p_state), 1);
        on_machines.insert(MachineId_t(i));
    }
    active_machines = Machine_GetTotal();
    
    // for(unsigned i = 0; i < active_machines; i++)
    //     vms.push_back(VM_Create(LINUX, X86));
    // for(unsigned i = 0; i < active_machines; i++) {
    //     machines.push_back(MachineId_t(i));
    // }    
    // for(unsigned i = 0; i < active_machines; i++) {
    //     VM_Attach(vms[i], machines[i]);
    // }

    // bool dynamic = false;
    // if(dynamic)
    //     for(unsigned i = 0; i<4 ; i++)
    //         for(unsigned j = 0; j < 8; j++)
    //             Machine_SetCorePerformance(MachineId_t(0), j, P3);
    // // Turn off the ARM machines
    // for(unsigned i = 24; i < Machine_GetTotal(); i++)
    //     Machine_SetState(MachineId_t(i), S5);

    //SimOutput("Scheduler::Init(): VM ids are " + to_string(vms[0]) + " ahd " + to_string(vms[1]), 3);
}

void Scheduler::MigrationComplete(Time_t time, VMId_t vm_id) {
    // Update your data structure. The VM now can receive new tasks
}

//Method that checks vm, memory, cpu availability and gpu requirements
bool CheckRequirements(MachineId_t machine_ID, TaskId_t task_id){
    bool gpu_capable = IsTaskGPUCapable(task_id);
    unsigned task_mem = GetTaskMemory(task_id);
    VMType_t req_vm = RequiredVMType(task_id);
    CPUType_t req_cpu_type = RequiredCPUType(task_id);
    unsigned int mem_with_task = task_mem + Machine_GetInfo(machine_ID).memory_used;
    unsigned int cores_with_task = 1 + Machine_GetInfo(machine_ID).active_tasks;
    return mem_with_task <= Machine_GetInfo(machine_ID).memory_size 
        && cores_with_task <= Machine_GetInfo(machine_ID).num_cpus && (Machine_GetInfo(machine_ID).gpus == gpu_capable || (Machine_GetInfo(machine_ID).gpus && !gpu_capable));
}

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // Get the task parameters
    SimOutput("NEW TASK INCOMING", 1);

    total_task.push_back(task_id);
    bool gpu_capable = IsTaskGPUCapable(task_id);
    unsigned task_mem = GetTaskMemory(task_id);
    VMType_t req_vm = RequiredVMType(task_id);
    SLAType_t req_sla = RequiredSLA(task_id);
    CPUType_t req_cpu_type = RequiredCPUType(task_id);
    // Decide to attach the task to an existing VM, 
    //if no machines are on or there none of the machines that are on are compatible 
    // Loop through active VMs
    for(auto &vm_id: vms){
        if(VM_GetInfo(vm_id).vm_type == req_vm){
            MachineId_t machine_id = VM_GetInfo(vm_id).machine_id;
            if(Machine_GetInfo(machine_id).p_state != S5 && VM_GetInfo(vm_id).vm_type == req_vm && Machine_GetInfo(machine_id).cpu == req_cpu_type 
                && CheckRequirements(machine_id, task_id)){
                //Task finally can be put on a VM and machine with ALL requirements
                SimOutput("In active VMs: Adding Task: " + to_string(task_id) + " to active machine: " + to_string(machine_id) 
                     + " to existing VM: " + to_string(vm_id), 1);
                SimOutput("Machine active tasks: " + to_string(Machine_GetInfo(machine_id).active_tasks), 1);
                SimOutput("Machine state: " + to_string(Machine_GetInfo(machine_id).p_state), 1);
                VM_AddTask(vm_id, task_id, LOW_PRIORITY);
                return;
            }
        }
    }
    // Haven't found active VM so loop through active machines that meet requirements
    for(auto &machine_id : on_machines){
        // Find machine that is active and compatible CPU and meets other requirements
        if(Machine_GetInfo(machine_id).p_state != S5 && Machine_GetCPUType(machine_id) == req_cpu_type && CheckRequirements(machine_id, task_id)){
            // Found Machine, Now create VM
            SimOutput("In active Machines: Adding Task: " + to_string(task_id) + " to active machine: " + to_string(machine_id) 
                        + " to new VM", 1);
            SimOutput("Machine active tasks: " + to_string(Machine_GetInfo(machine_id).active_tasks), 1);
            SimOutput("Machine state: " + to_string(Machine_GetInfo(machine_id).p_state), 1);
            VMId_t vm = VM_Create(req_vm, req_cpu_type);
            vms.push_back(vm); //Doesnt save the VM_ID
            VM_Attach(vms.back(), machines[machine_id]); //worried about migration
            VM_AddTask(vms.back(), task_id, LOW_PRIORITY);
            return;
        }
    }
    // Didn't find active machines that meet requirements
    // No CPU was found in the ON machines so loop through OFF machines
    for (auto it = off_machines.begin(); it != off_machines.end(); ) {
        int machine_id = *it;
        if(Machine_GetCPUType(machine_id) == req_cpu_type && CheckRequirements(machine_id, task_id)){
            // Found Machine, Now create VM
            Machine_SetState(machine_id, S0); // Power up the machine
            CPUPerformance_t current_p = Machine_GetInfo(machine_id).p_state;
        
            on_machines.insert(machine_id); // Update machine to on
            SimOutput("In off Machines: Adding Task: " + to_string(task_id) + " to off machine: " + to_string(machine_id)                             + " to new VM", 1);
            SimOutput("Machine active tasks: " + to_string(Machine_GetInfo(machine_id).active_tasks), 1);
            SimOutput("Machine state: " + to_string(Machine_GetInfo(machine_id).p_state), 1);
            vms.push_back(VM_Create(req_vm, req_cpu_type)); //Doesnt save the VM_ID
            //VM_AddTask(vms.back(), task_id, LOW_PRIORITY);
            //it = off_machines.erase(it);
            // Increment active machine count and exit loop
            //active_machines++;
            return;
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
    //if number of tasks on a machine is 0, turn off machine
    // for (auto it = on_machines.begin(); it != on_machines.end(); ) {
    //         int machine_id = *it;
    //         if(Machine_GetInfo(machine_id).active_tasks == 0){
    //             // off_machines.insert(machine_id);
    //             // it = on_machines.erase(it);
    //             Machine_SetState(machine_id, S5);
    //             // Increment active machine count and exit loop
    //             //active_machines--;
    //         } else {
    //             ++it; // Move to the next machine
    //             }
    //     }
    // for(auto &machine_id : on_machines){
    //     SimOutput("Machine: " + to_string(machine_id), 4);
    //     unsigned num_tasks = Machine_GetInfo(machine_id).active_tasks;
    //     if(num_tasks==0){
    //         SimOutput("Machine State: " + to_string(Machine_GetInfo(machine_id).p_state), 4);
    //         // for(auto &vm : vms){
    //         //     if(VM_GetInfo(vm).machine_id == machine_id){
    //         //         VM_Shutdown(vm);
    //         //     }
    //         // }   
    //     }
    // }
    //  for(auto &vm : vms){
    //     MachineId_t machine = VM_GetInfo(vm).machine_id;
    //     unsigned num_tasks = VM_GetInfo(vm).active_tasks.size();
    //     SimOutput("VM: " + to_string(vm), 4);
    //     SimOutput("Machine: " + to_string(machine), 4);
    //     SimOutput("Number of tasks: " + to_string(num_tasks), 4);
    //     if(num_tasks!=0){
    //         SimOutput("Machine State: " + to_string(Machine_GetInfo(machine).p_state), 4);
    //         for(auto &task : VM_GetInfo(vm).active_tasks){
    //                 SimOutput("TaskID: " + to_string(task) + ", remaing instr: " + to_string(GetTaskInfo(task).remaining_instructions), 4);
    //         }
    //     }
    // }
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
    static unsigned counts = 0;
    counts++;
    if(counts == 10) {
         migrating = true;
        //Going through active machines and seeing their active tasks
         //ThrowException("Migrating, total tasks: ", total_task.size());
         //VM_Migrate(1, 9);
    }
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
     SimOutput("In StateChange , Time: " + to_string(time), 1);
    // Called in response to an earlier request to change the state of a machine
    if(on_machines.size()==0 && off_machines.size()==0){
    SimOutput("Init Machine " + to_string(machine_id) + " is now active.", 1);
        if (Machine_GetInfo(machine_id).p_state == S0){
            on_machines.insert(machine_id);
            active_machines++;
        }
        if (Machine_GetInfo(machine_id).p_state == S5) {
            off_machines.insert(machine_id);
            SimOutput("Init Machine " + to_string(machine_id) + " is now inactive.", 1);
        }
    }else{
    if (Machine_GetInfo(machine_id).p_state == S0) {
        on_machines.insert(machine_id);
        off_machines.erase(machine_id);
        SimOutput("Machine " + to_string(machine_id) + " is now active.", 1);
        active_machines++;

    }
    if (Machine_GetInfo(machine_id).p_state == S5) {
        on_machines.erase(machine_id);
        off_machines.insert(machine_id);
        SimOutput("Machine " + to_string(machine_id) + " is now inactive.", 1);
        active_machines--;
    }
    }
}

