package com.vkc;

import twitter4j.*;

import java.util.concurrent.LinkedBlockingQueue;

public class TwitterStatusListener implements StatusListener {

    public LinkedBlockingQueue<Status> getQueue() {
        return queue;
    }

    LinkedBlockingQueue<Status> queue = new LinkedBlockingQueue<Status>(1000);
    @Override
    public void onStatus(Status status) {
        queue.offer(status);

   /*     System.out.println("@" + status.getUser().getScreenName() + " - " + status.getText());
         System.out.println("@" + status.getUser().getScreenName());

            for(URLEntity urle : status.getURLEntities()) {
               System.out.println(urle.getDisplayURL());
            }

            for(HashtagEntity hashtage : status.getHashtagEntities()) {
               System.out.println(hashtage.getText());
            }*/
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
        System.out.println("Got a status deletion notice id:"
        + statusDeletionNotice.getStatusId());
    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
        System.out.println("Got track limitation notice:" +
        numberOfLimitedStatuses);
    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {
        System.out.println("Got scrub_geo event userId:" + userId +
        "upToStatusId:" + upToStatusId);
    }

    @Override
    public void onStallWarning(StallWarning stallWarning) {
         System.out.println("Got stall warning:" + stallWarning);
    }

    @Override
    public void onException(Exception e) {
        e.printStackTrace();
    }
}
