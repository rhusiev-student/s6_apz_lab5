The repo: https://github.com/rhusiev-student/s6_apz_lab5

Here is how the setup looks like:

![setup](img/setup.png)

Now there are scripts to launch each service.

Here are some sample requests:

![requests](img/requests.png)

If some services fail (even with sigkill, with no chance to gracefully shutdown):

![kill](img/kill.png)

(You can even see failed checkmarks on the dashboard, with healthchecks added.)

And requests are going to the respecting service:

![after kill](img/after_kill.png)

And the services can be restarted - everything returns to normal:

![restart failed services](img/restart_failed.png)

If some services fail gracefully, just fewer instances exist (no need for red checkmarks, the service just eliminated itself):

![fail gracefully](img/fail_gracefully.png)

