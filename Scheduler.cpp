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
map<CPUType_t, vector<VMType_t>> cpu_VM_Mapping = {
        {ARM, {LINUX, LINUX_RT, WIN}},
        {POWER, {LINUX, LINUX_RT, AIX}},
        {RISCV, {LINUX, LINUX_RT}},
        {X86, {LINUX, LINUX_RT, WIN}}
    };
map<MachineId_t, vector<VMType_t>> vms_On_Machine;

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
        Machine_SetState(MachineId_t(i), S5);
        //SimOutput("machine id: " + MachineId_t(i), 1);
        off_machines.insert(MachineId_t(i));
    }
    active_machines = 0;
    SimOutput("bur?", 1);
    
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

void Scheduler::NewTask(Time_t now, TaskId_t task_id) {
    // Get the task parameters
    SimOutput("NEW TASK INCOMING", 1);
    bool gpu_capable = IsTaskGPUCapable(task_id);
    unsigned task_mem = GetTaskMemory(task_id);
    VMType_t req_vm = RequiredVMType(task_id);
    SLAType_t req_sla = RequiredSLA(task_id);
    CPUType_t req_cpu_type = RequiredCPUType(task_id);
    // Decide to attach the task to an existing VM, 
    //if no machines are on or there none of the machines that are on are compatible 
    if (on_machines.empty()){
        //Iterate through list of off machines and then select associated CPU
        for(const auto& machine_ID : off_machines){
            ////SimOutput(to_string(machine_ID), 1);
            //Check machines CPU type
            SimOutput(to_string(machine_ID), 1);
            if(Machine_GetCPUType(machine_ID) == req_cpu_type){
                Machine_SetState(machine_ID, S0);
                vms.push_back(VM_Create(req_vm, req_cpu_type)); //Doesnt save the VM_ID
                VM_Attach(vms.back(), machines[machine_ID]); //worried about migration
                VM_AddTask(vms.back(), task_id, LOW_PRIORITY);
                on_machines.insert(machine_ID);
                vms_On_Machine.insert({machine_ID, {req_vm}}); // Adding the Req_VM to the unique Machine_ID
                off_machines.erase(machine_ID);
                active_machines++;
                break;
            }
        }
    }
    // Now checking if active machines have the desired CPU and VM
    else
    {
        for (const auto& machine_ID : on_machines){
            //SimOutput("machine: " + machine_ID, 1);
            //Filtering Based on Type of CPU
            if(Machine_GetCPUType(machine_ID) == req_cpu_type){
                //Filter Based on VMs
                vector<VMType_t> active_vms = vms_On_Machine.at(machine_ID);
                bool success = false;
                for(const auto& vm : active_vms){
                    // Found a VM
                    if(vm == req_vm){
                        //Now check memory
                        unsigned mem_with_task = task_mem + Machine_GetInfo(machine_ID).memory_used;
                        //If memory with the task is less than total memory
                        if(mem_with_task <= Machine_GetInfo(machine_ID).memory_size){
                            //Now filtering based on Cores
                            unsigned cores_with_task = 1 + Machine_GetInfo(machine_ID).active_tasks;
                            if(cores_with_task <= Machine_GetInfo(machine_ID).num_cpus){
                                //Task finally can be put on the machine with ALL requirements
                                VM_AddTask(vm, task_id, LOW_PRIORITY);
                                success = true;
                                //Breaks out of looping through VMs in an active machine
                                break;
                            }
                        }
                    }
                } 
                //Breaks out of looping through active machines
                if(success){   
                    break;
                }
                // No vm was successfully found, need to add a new one
                // Check memory and available cores of active machines
                unsigned mem_with_task = task_mem + Machine_GetInfo(machine_ID).memory_used;
                //If memory with the task is less than total memory
                if(mem_with_task <= Machine_GetInfo(machine_ID).memory_size){
                    //Now filtering based on Cores
                    unsigned cores_with_task = 1 + Machine_GetInfo(machine_ID).active_tasks;
                    if(cores_with_task <= Machine_GetInfo(machine_ID).num_cpus){
                        //Task finally can be put on an active machine with CPU, Mem, and Core requirements
                        vms.push_back(VM_Create(req_vm, req_cpu_type)); //Doesnt save the VM_ID
                        VM_Attach(vms.back(), machines[machine_ID]); //worried about migration
                        VM_AddTask(vms.back(), task_id, LOW_PRIORITY);
                        on_machines.insert(machine_ID);
                        off_machines.erase(machine_ID);
                        success = true;
                        // In the active machines found the requirements to add a VM and task so exit
                        break;
                    }
                }
                // vms.push_back(VM_Create(req_vm, req_cpu_type)); //Doesnt save the VM_ID
                // VM_Attach(vms.back(), machines[machine_ID]); //worried about migration
                // VM_AddTask(vms.back(), task_id, LOW_PRIORITY);
                // on_machines.insert(machine_ID);
                // off_machines.erase(machine_ID);
            }
        }
        // No CPU was found in the ON machines so loop through OFF machines
        for(const auto& machine_ID : off_machines){
            //Check machines CPU type
            if(Machine_GetCPUType(machine_ID) == req_cpu_type){
                Machine_SetState(machine_ID, S0);
                vms.push_back(VM_Create(req_vm, req_cpu_type)); //Doesnt save the VM_ID
                VM_Attach(vms.back(), machines[machine_ID]); //worried about migration
                VM_AddTask(vms.back(), task_id, LOW_PRIORITY);
                on_machines.insert(machine_ID);
                vms_On_Machine.insert({machine_ID, {req_vm}}); // Adding the Req_VM to the unique Machine_ID
                off_machines.erase(machine_ID);
                active_machines++;
                break;
            }
        }
        

    }
    
    
    //      vm.AddTask(taskid, Priority_T priority); or
    // Create a new VM, attach the VM to a machine
    //      VM vm(type of the VM)
    //      vm.Attach(machine_id);
    //      vm.AddTask(taskid, Priority_t priority) or
    // Turn on a machine, create a new VM, attach it to the VM, then add the task
    //
    // Turn on a machine, migrate an existing VM from a loaded machine....
    //
    // Other possibilities as desired
    // Priority_t priority = (task_id == 0 || task_id == 64)? HIGH_PRIORITY : MID_PRIORITY;
    // if(migrating) {
    //     VM_AddTask(vms[0], task_id, priority);
    // }
    // else {
    //     VM_AddTask(vms[task_id % active_machines], task_id, priority);
    // }// Skeleton code, you need to change it according to your algorithm
}

void Scheduler::PeriodicCheck(Time_t now) {
    // This method should be called from SchedulerCheck()
    // SchedulerCheck is called periodically by the simulator to allow you to monitor, make decisions, adjustments, etc.
    // Unlike the other invocations of the scheduler, this one doesn't report any specific event
    // Recommendation: Take advantage of this function to do some monitoring and adjustments as necessary


    //if number of tasks on a machine is 0, turn off machine
    
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
        VM_Migrate(1, 9);
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
    // Called in response to an earlier request to change the state of a machine
}

