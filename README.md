# Simple Nova's Scheduler Simulator

## Installation

The scheduler needs OpenStack nova to run. Install a virtualenv and install
nova into it.

    virtualenv --system-site-packages .venv
    source .venv/bin/activate
    pip install -e /path/to/nova


## Trace file

You'll need a trace file to actually create the simulation. Create one with
the following format:

    id,user,submit,start,end,terminate,cores,image_id,image_size,flavor

This file is read by defualt from `data/trace.dat`. Please note the following:

- `submit`, `start`, `end` and `terminate` are seconds.

- `submit` is the time when the request will be submitted.

- `terminate` indicates how many seconds will pass until the
  instances are terminated and the request is cancelled. This is an
  optional value and if it is not specified the images won't be
  terminated.

- `start` and `end` are optional values. They are used to simulate
  the execution of a job insided the allocated instances. Therefore
  `submit` < `start` < `end`, otherwise the request will be discarded.

- `cores` is the number of cores that the user needs. The simulator
  will automatically request as many machines as needed to satisfy
  this demand.

- `image_id` is the image id to be used. The option `default_image`
  will be used if this field is empty.

- `image_size` is the size of the image to be used. The option
  `default_image_size` will be used if this field is empty.

- `flavor` is the instance type (flavor) that will be used. The
  option `default_flavor` will be used if this field is empty.

## Configuration

### Nova configuration

The simulator needs to read the `nova.conf` configuration so that it can get
the scheduler configuration. You can specify it by passing it with the
`--config-file` command line option. If you do not specify it, the defaults
will be used

### Simulation configuration

You can pass another configuration file with the `--config-file` command line
option to configure various parameters for the simulation. Have a look at
`etc/simulator.conf.sample` for the available configuration options.

## Execution

After enabling the virtualenv, run the simulator like this:

    source .venv/bin/activate
    python -B simulate.py --config-file etc/nova.conf --config-file etc/simulation.conf
