# Original decay function

A function that crates a linear decay function y = F(x), where a *smooth* rationing
(k per time slice) of population (n) of units of work into discrete time slices (t)
is converted into decay-based allocation with larger allocations of work units per slice
in early time slices and tapering off of work unit allocation in later time slices

Note that at z=2 there are no remaining empty time slices at the end. With z higher than 2
you are building a comfy padding of empty time slices at the end. With z < 2 you overshoot
t and will not have enough slices at the end to burn off entire population of tasks.
In order to prevent silly values of z, see assert further below

Used to allow aggressive-from-start oozing out of tasks in the beginning of
processing period. Also allows for a gap of time (between r and t) where
long-trailing tasks can finish and API throttling threshold to recover.

```
zk|                      |
  |`-,_                  |
y |----i-,_              |
k |----|-----------------|
  |    |       `-,_      |
  |____|___________`-,___|
 0     x             r   t
```

Homework (please check my homework and poke me in the eye. DDotsenko):

The task was to derive computationally-efficient decay function (exponential would be cool,
but too much CPU for little actual gain, so settled on linear) for flushing out tasks.

What's known in the beginning:
- Total number of tasks - n
- total number of periods we'd like to push the tasks over - t
  (say, we want to push out tasks over 10 minutes, and we want to do it
   every second, so 10*60=600 total periods)

The approach taken is to jack up by multiplier z the original number of pushed out tasks
in the very fist time slice compared to - k = n / t - what would have been pushed out if all the tasks
were evenly allocated over all time slices.

This becomes a simple "find slope of hypotenuse (zk-r)" problem, where we know only two things about
that triangle:
- rise is k*z (kz for short)
- area of the triangle (must be same as area of k-t rectangle) - n - the total population

To find out r let's express the area of that triangle as half-area of a rectangle zk-r
    n = zkr / 2

From which we derive r:
    r = 2n / zk

Thus, equation for slope of the hypotenuse can be computed as:
    y = zk - zk/r * x

Which reduces to:
    y = zk - (zk^2 / 2n) * x


## Original function to calculate area under curve (integral)

```
    Computes total population ("area") of tasks we should have already processed
    by the time we are at this value of x

    zk|                      |
      |`-,_ P                |
    y |----i-,_              |
    k |----|-----------------|
      |    |       `-,_      |
      |____|___________`-,___|
     0     x             r   t

    The area is zk-P-x-0
```
